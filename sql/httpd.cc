// Copyright (c) 2008 Google Inc. All Rights Reserved.

/*
  A lightweight HTTP server for serving application health through
  /health, /var, /status etc.

  Don't try to think of this as a web-server, as there is more code
  here to deal with reading/writing to sockets than checking a GET
  request.

  If you want to add support for a new URL, then make your changes
  in httpd_process_request().  All other work is done in httpd_request.cc.
*/

#include <ctype.h>
#include <my_global.h>
#include <my_sys.h>
#include <my_net.h>
#include <my_pthread.h>
#include <thr_alarm.h>
#include <mysql_com.h>
#include <violite.h>

#include "mysql.h"
#include "mysql_priv.h"
#include "my_sys.h"
#include "httpd.h"

ulong httpd_thread_created;
ulonglong httpd_thread_count;
ulong httpd_bind_addr;                        /* HTTP server address */
char *httpd_bind_addr_str;                    /* HTTP server address string */
uint httpd_port= 8080;                        /* HTTP server port */
/* True if HTTP server is turned on. */
bool httpd= false;
/* True if we trust the users to use /qqq and /aaa. */
my_bool httpd_trust_clients= false;
char *httpd_unix_port;

my_socket httpd_sock;
my_socket httpd_unix_sock;


/**
   Perform either a blocking or non-blocking read from the client.
   The function will switch to blocking mode, if non-blocking reads fails
   or any reason.
*/

static bool httpd_net_read(NET *net)
{
  thr_alarm_t alarmed;
  ALARM alarm_buff;
  thr_alarm_init(&alarmed);

  /*
    First determine if we're reading from the socket in blocking
    or non-blocking (O_NONBLOCK) mode.
  */
  bool net_blocking= vio_is_blocking(net->vio);

  /*
    In blocking mode we set a timeout to ensure we don't get stuck
    reading forever.
  */
  if (net_blocking)
    thr_alarm(&alarmed, (uint)net->write_timeout, &alarm_buff);

  uint32_t length= net->buff_end - net->buff;
  uint32_t retry_count= 0;

  /*
    Fill the preallocated net->buff with content until we hit CRLFCRLF
    in the input stream, or timeout, or hit the maximum input.

    Nothing particularly concerns us, other than the first line -- the GET
    request -- all other statements are ignored.
  */
  unsigned char *buff= net->buff;
  bool last_chunk= false;

  while (! last_chunk && length > 0)
  {
    int read_len= vio_read(net->vio, (uchar *) buff, length);
    if (read_len <= 0)
    {
      bool interrupted= vio_should_retry(net->vio);

      if ((interrupted || read_len == 0) && !thr_alarm_in_use(&alarmed))
      {
        if (! thr_alarm(&alarmed, net->read_timeout, &alarm_buff))
        {
          /*
            As we were unsuccessful in reading from the socket in
            non-blocking mode, we will now switch to blocking-mode
            and try again.
          */
          my_bool old_mode;
          while (vio_blocking(net->vio, true, &old_mode) < 0)
          {
            if (vio_should_retry(net->vio) &&
                retry_count++ < net->retry_count)
              continue;

            net->error= 2;
            net->last_errno= ER_NET_FCNTL_ERROR;
            my_error(net->last_errno, MYF(0));
            goto end;
          }
          retry_count= 0;
          continue;
        }
      }

      if (thr_alarm_in_use(&alarmed) && !thr_got_alarm(&alarmed)
          && interrupted)
      {
        if (retry_count++ < net->retry_count)
          continue;
      }

      if (vio_errno(net->vio) == SOCKET_EINTR)
        continue;
      net->error= 2;                          /* Close socket. */

      net->last_errno= (vio_was_interrupted(net->vio)
                        ? ER_NET_READ_INTERRUPTED
                        : ER_NET_READ_ERROR);
      my_error(net->last_errno, MYF(0));
      goto end;
    }

    /*
      If we've reached here, then we've successfully read something from
      the socket into a buffer.
    */
    buff+= read_len;
    length-= read_len;

    /* HTTP requests are terminated by two CRLFs. */
    if (buff - net->buff >= 4)
    {
      if (strncmp((char *) buff - 4, "\r\n\r\n", 4) == 0)
        last_chunk= true;
    }
  }
end:
  if (thr_alarm_in_use(&alarmed))
  {
    thr_end_alarm(&alarmed);
    /* Set the socket back to it's original mode. */
    my_bool old_mode;
    vio_blocking(net->vio, net_blocking, &old_mode);
  }
  return last_chunk;
}


static bool httpd_net_write(NET *net, const char *buffer, ulong len)
{
  ulong left_length= (ulong) (net->buff_end - net->write_pos);

#ifdef DEBUG_DATA_PACKETS
  DBUG_DUMP("data", packet, len);
#endif
  if (len > left_length)
  {
    if (net->write_pos != net->buff)
    {
      /* Fill up already used packet and write it. */
      memcpy((char*) net->write_pos, buffer, left_length);
      if (net_real_write(net, (uchar *) net->buff,
                         (ulong) (net->write_pos - net->buff) + left_length))
        return true;
      net->write_pos= net->buff;
      buffer += left_length;
      len -= left_length;
    }
    if (len > net->max_packet)
      return net_real_write(net, (uchar *) buffer, len) ? true : false;
    /* Send out rest of the blocks as full sized blocks. */
  }
  memcpy((char*) net->write_pos, buffer, len);
  net->write_pos += len;
  return false;
}


static bool httpd_net_write(NET *net, const char *buffer)
{
  return httpd_net_write(net, buffer, strlen(buffer));
}


/**
   Listen for one HTTP GET request.  Once the GET line has been found,
   return.  The NET buffer will contain the buffer data.
*/

static void httpd_read_request(THD *thd)
{
  DBUG_ENTER("HTTPServer::readRequest");
  NET *net= &thd->net;
  thd->clear_error();

  /*
    This thread will do a blocking read from the client which
    will be interrupted when the next command is received from
    the client, the connection is closed or "net_wait_timeout"
    number of seconds has passed.
  */
  my_net_set_read_timeout(net, thd->variables.net_wait_timeout);

  /*
    Read the incoming HTTP request.
    The request received will typically look like this:

    GET /foo/bar/status HTTP/1.1
    Host: localhost:9999
    User-Agent: Mozilla/5.0 Gecko/20060201 Firefox/2.0.0.6 (Ubuntu-dapper)
    Accept: text/xml,text/html;q=0.9,text/plain;q=0.8,image/png,;q=0.5
    Accept-Language: en-gb,en;q=0.7,en-us;q=0.3
    Accept-Encoding: gzip,deflate
    Accept-Charset: ISO-8859-1,utf-8;q=0.7,*;q=0.7
    Keep-Alive: 300
    Connection: keep-alive

    We are only interested in the first line.
  */
  httpd_net_read(net);
  DBUG_VOID_RETURN;
}


static void httpd_end_thread(THD *thd)
{
  thd->cleanup();
  pthread_mutex_lock(&LOCK_thread_count);
  httpd_thread_count--;
  pthread_mutex_unlock(&LOCK_thread_count);
  delete thd;

  /* It's safe to broadcast outside a lock (COND... is not deleted here). */
  pthread_cond_broadcast(&COND_thread_count);
#ifdef ONE_THREAD
  if (!(test_flags & TEST_NO_THREADS))        /* For debugging under Linux. */
#endif
  {
    my_thread_end();
    pthread_exit(0);
  }
}


static bool match_url(const NET *net, const char *url)
{
  return strncmp((const char *) net->buff, url, strlen(url)) == 0;
}


/**
   Process the NET buffer populated by httpd_read_request and action
   the GET request accordingly.  'req' is filled with the response
   body and header suitable for sending back to the client.
*/

static int httpd_process_request(THD *thd, Http_request *req)
{
  NET *net= &thd->net;
  int err= 0;

  if (httpd_trust_clients && match_url(net, "GET /quitquitquit"))
  {
    req->generate_header(200, "Going down... NOW");
    req->quitquitquit();
  }
  else if (httpd_trust_clients && match_url(net, "GET /abortabortabort"))
  {
    req->abortabortabort();
  }
  else if (match_url(net, "GET /var"))
  {
    req->parse_url_params();
    err= req->var();
    if (!err)
      req->generate_header(200, false);
  }
  else if (match_url(net, "GET /health"))
  {
    req->health();
    req->generate_header(200, true);
  }
  else if (match_url(net, "GET /growth"))
  {
    req->growth();
    req->generate_header(200, false);
  }
  else if (match_url(net, "GET /heap"))
  {
    req->heap();
    req->generate_header(200, false);
  }
  else if (match_url(net, "GET /release_memory"))
  {
    req->release_memory();
    req->generate_header(200, false);
  }
  else if (match_url(net, "GET /malloc"))
  {
    req->malloc();
    req->generate_header(200, false);
  }
  else if (match_url(net, "GET /status") || match_url(net, "GET /"))
  {
    req->status();
    req->generate_header(200, true);
  }
  else
  {
    req->generate_header(404, false);
    req->generate_error("Unknown URL");
    return 0;
  }
  return err;
}


/**
   Publish the previously generated HTTP respose, sending the data
   back to the client.
*/

static void httpd_send_response(THD *thd, Http_request *req)
{
  NET *net= &thd->net;
  httpd_net_write(net, req->response_header(), req->response_header_length());
  httpd_net_write(net, req->response_body(), req->response_body_length());
  net_flush(net);
}


/**
   Send a error string to client.

   Design note:

   net_printf_error and net_send_error are low-level functions
   that shall be used only when a new connection is being
   established or at server startup.
   For SIGNAL/RESIGNAL and GET DIAGNOSTICS functionality it's
   critical that every error that can be intercepted is issued in one
   place only, my_message_sql.
*/

static void httpd_send_error(THD *thd, const char *err)
{
  NET *net= &thd->net;

  httpd_net_write(net, "HTTP/1.0 404 Not Found\r\n");
  httpd_net_write(net, "Content-Type: text/plain; charset=UTF-8\r\n");
  httpd_net_write(net, "Server: mysqld\r\n");
  httpd_net_write(net, "Connection: Close\r\n\r\n\r\n");
  httpd_net_write(net, "HTTP Server failure:\r\n");
  httpd_net_write(net, err, strlen(err));
  net_flush(net);
  thd->is_fatal_error= 0;                     /* Error message is given. */
}


/**
   Close a HTTP connection.

   @param  thd        thread handle
   @param  errcode    error code to print to console
   @param  lock       1 if we have have to lock LOCK_thread_count

   For the connection that is doing shutdown, this is called twice.
*/

static void httpd_close_connection(THD *thd, uint errcode, bool lock)
{
  if (lock)
    pthread_mutex_lock(&LOCK_thread_count);
  thd->killed= THD::KILL_CONNECTION;
  st_vio *vio;
  if ((vio= thd->net.vio) != 0)
  {
    if (errcode)
    {
      httpd_send_error(thd, ER(errcode));
    }
    vio_close(vio);                           /* vio is freed in delete thd. */
  }
  if (lock)
    pthread_mutex_unlock(&LOCK_thread_count);
}


static my_socket httpd_create_socket(int httpd_flags, my_socket sock)
{
  my_socket new_sock;
  static uint error_count= 0;
  struct sockaddr_in cAddr;
  size_socket dummyLen;
  struct sockaddr dummy;

  for(uint retry= 0; retry < MAX_ACCEPT_RETRY; retry++)
  {
    size_socket length= sizeof(struct sockaddr_in);

    new_sock= accept(sock,
                     my_reinterpret_cast(struct sockaddr *) (&cAddr),
                     &length);

    if (new_sock != INVALID_SOCKET
        || (socket_errno != SOCKET_EINTR && socket_errno != SOCKET_EAGAIN))
      break;
#if !defined(NO_FCNTL_NONBLOCK)
    if (!(test_flags & TEST_BLOCKING))
    {
      /* Try without O_NONBLOCK. */
      if (retry == MAX_ACCEPT_RETRY - 1)
        fcntl(sock, F_SETFL, httpd_flags);
    }
#endif
  }

#if !defined(NO_FCNTL_NONBLOCK)
  if (!(test_flags & TEST_BLOCKING))
    fcntl(sock, F_SETFL, httpd_flags);
#endif
  if (new_sock == INVALID_SOCKET)
  {
    if ((error_count++ & 255) == 0)
      /* This can happen often. */
      sql_perror("Error in accept");
    if (socket_errno == SOCKET_ENFILE || socket_errno == SOCKET_EMFILE)
      /* Give other threads some time. */
      sleep(1);

    return INVALID_SOCKET;
  }

  dummyLen= sizeof(struct sockaddr);

  if (getsockname(new_sock, &dummy, &dummyLen) < 0)
  {
    sql_perror("Error on new connection socket");
    shutdown(new_sock, SHUT_RDWR);
    closesocket(new_sock);
    return INVALID_SOCKET;
  }

  return new_sock;
}


static THD *httpd_create_thd(my_socket new_sock)
{
  if (!my_thread_init())
  {
    /* Don't allow too many connections. */
    THD *thd= new THD;
    if (!thd)
    {
      return NULL;
    }
    st_vio *vio_tmp= vio_new(new_sock, VIO_TYPE_TCPIP, 0);
    if (!vio_tmp || my_net_init(&thd->net, vio_tmp))
    {
      if (vio_tmp)
        vio_delete(vio_tmp);

      delete thd;
      thd= NULL;
    }
    return thd;
  }
  return NULL;
}


/**
   Initializes the THD object for a thread.

   @return
     @retval  0   success
     @retval  1   error
*/

static int httpd_setup_thd(THD *thd)
{
  NET *net= &thd->net;
  DBUG_ENTER("httpd_setup_thd");
  net->return_errno= 1;

  /* Don't allow too many connections. */
  if (thread_count + httpd_thread_count > max_connections)
  {
    DBUG_PRINT("error", ("Too many connections"));
    DBUG_RETURN(1);
  }

  if (abort_loop)
  {
    DBUG_RETURN(1);
  }

  pthread_mutex_lock(&LOCK_thread_count);
  thd->thread_id= thread_id++;
  thd->real_id= pthread_self();

  httpd_thread_count++;
  httpd_thread_created++;
  pthread_mutex_unlock(&LOCK_thread_count);
  DBUG_RETURN(0);
}


/**
   Thread start function.  This will setup the thread stack,
   read from the socket, process the request and return a result
   to the client.
*/

pthread_handler_t httpd_handle_connection(void *arg)
{
  pthread_detach_this_thread();

  sigset_t set;
  sigemptyset(&set);                          /* Get mask in use. */
  pthread_sigmask(SIG_UNBLOCK, &set, NULL);

  /*
    Without the additional casting to long for new_socket the
    compilations terminates with:

        error: cast from 'void*' to 'my_socket {aka int}'
        loses precision [-fpermissive]

    This casting it safe because we only compile for 64-bit.
  */
  my_socket new_socket= (my_socket) (long) arg;
  THD *thd= httpd_create_thd(new_socket);

  if (!thd || httpd_setup_thd(thd))
  {
    shutdown(new_socket, SHUT_RDWR);
    closesocket(new_socket);
    httpd_close_connection(thd, ER_OUT_OF_RESOURCES, 1);
    httpd_end_thread(thd);
    return 0;
  }

  thd->thread_stack= (char*) &thd;

  if (thd->store_globals())
  {
    httpd_close_connection(thd, ER_OUT_OF_RESOURCES, 1);
    httpd_end_thread(thd);
    return 0;
  }


  /* RESTRICTED SCOPE */ {
    Http_request req(thd);
    NET *net= &thd->net;

    /* Use "connect_timeout" value during connection phase. */
    my_net_set_read_timeout(net, connect_timeout);
    my_net_set_write_timeout(net, connect_timeout);

    thd_proc_info(thd, 0);
    thd->command= COM_SLEEP;
    thd->set_time();

    /* Connect completed, set read/write timeouts back to tdefault. */
    my_net_set_read_timeout(net, thd->variables.net_read_timeout);
    my_net_set_write_timeout(net, thd->variables.net_write_timeout);

    httpd_read_request(thd);
    int error= httpd_process_request(thd, &req);
    if (! error)
      httpd_send_response(thd, &req);
    httpd_close_connection(thd, error, 1);
  }

  /* This call to end_thread should never return.  */
  httpd_end_thread(thd);
  /* If end_thread returns, we are either running with --one-thread. */
  thd= current_thd;
  thd->thread_stack= (char*) &thd;

  return 0;
}


/**
   A separate handler is used for TCP/IP connections to the HTTP server
   thread so that clients can access status information while the server
   is initialsing the storage engines.  InnoDB can take a very long time
   to start, if the transaction logs need processing, so it is important
   to be able to get status information that indicates the server is actually
   alive early on, rather than waiting 10-30 minutes for a result.
*/

pthread_handler_t handle_httpd_connections(void *arg __attribute__((unused)))
{
  DBUG_ENTER("handle_httpd_connections");
  my_pthread_getprio(pthread_self());
  fd_set clientFDs;
  int httpd_flags= 0;
  int httpd_unix_flags= 0;
  my_socket sock;
  int flags;

  FD_ZERO(&clientFDs);
  if (httpd_sock != INVALID_SOCKET)
  {
    FD_SET(httpd_sock, &clientFDs);
#ifdef HAVE_FCNTL
    httpd_flags= fcntl(httpd_sock, F_GETFL, 0);
#endif
  }
#ifdef HAVE_SYS_UN_H
  if (httpd_unix_sock != INVALID_SOCKET)
  {
    FD_SET(httpd_unix_sock, &clientFDs);
#ifdef HAVE_FCNTL
    httpd_unix_flags= fcntl(httpd_unix_sock, F_GETFL, 0);
#endif
  }
#endif /* HAVE_SYS_UN_H */

  while (!abort_loop)
  {
    fd_set readFDs= clientFDs;
    if (select(max(httpd_sock, httpd_unix_sock) + 1, &readFDs, 0, 0, 0) < 0)
    {
      if (socket_errno != SOCKET_EINTR)
      {
        if (!select_errors++ && !abort_loop)
          sql_print_error("mysqld: Got error %d from select", socket_errno);
      }
      continue;
    }

    if (abort_loop)
      break;

#ifdef HAVE_SYS_UN_H
    if (httpd_unix_sock != INVALID_SOCKET &&
        FD_ISSET(httpd_unix_sock, &readFDs))
    {
      sock= httpd_unix_sock;
      flags= httpd_unix_flags;
    }
    else
#endif
    if (httpd_sock != INVALID_SOCKET && FD_ISSET(httpd_sock, &readFDs))
    {
      sock= httpd_sock;
      flags= httpd_flags;
    }

    /* This is a new connection request. */
#if !defined(NO_FCNTL_NONBLOCK)
    if (!(test_flags & TEST_BLOCKING))
    {
#if defined(O_NONBLOCK)
      fcntl(sock, F_SETFL, flags | O_NONBLOCK);
#elif defined(O_NDELAY)
      fcntl(sock, F_SETFL, flags | O_NDELAY);
#endif
    }
#endif /* NO_FCNTL_NONBLOCK */

    my_socket new_socket= httpd_create_socket(flags, sock);

    if (new_socket == INVALID_SOCKET)
      continue;

    pthread_t thr;
    /*
      Without the additional casting to long for new_socket the
      compilations terminates with:

          error: cast to pointer from integer of different
          size [-Werror=int-to-pointer-cast]

      This casting it safe because we only compile for 64-bit.
    */
    if (pthread_create(&thr, &connection_attrib, httpd_handle_connection,
                       (void *) (long) new_socket))
      sql_print_warning("Cannot create HTTP connection thread.");
  }
  DBUG_RETURN(0);
}


void create_httpd_thread()
{
  pthread_t thr;
  if (pthread_create(&thr, &connection_attrib,
                     handle_httpd_connections, 0))
    sql_print_warning("Cannot create the HTTP server thread.");
}
