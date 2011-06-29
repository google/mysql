// Copyright 2008 Google Inc. All Rights Reserved.

/*
  This unit deals with the collection of data from MySQL's internal
  data-structures into two main formats:
    1. key-value pairs for /var
    2. HTML tables for /status pages.

  Also provided are /quitquitquit, which cleanly shuts down the server,
  and /abortabortabort, which kills the server outright.
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
#include "slave.h"
#include "sql_repl.h"
#include "httpd.h"
#include "rpl_mi.h"

#define MAX_PREFIX_LENGTH 80                 // somewhat arbitrary


bool HTTPRequest::WriteTableHeader(const char *title,
                                   const char *const *headings)
{
  bool err= false;
  err|= WriteBody("<p><table bgcolor=#eeeeff width=100%><tr align=center>"
                   "<td><font size=+2>");
  err|= WriteBody(title);
  err|= WriteBody("</font></td></tr></table></p>"
                   "<table bgcolor=#fff5ee>\r\n<tr bgcolor=#eee5de>\r\n");
  while (*headings && !err)
  {
    err|= WriteBody("  <th>");
    err|= WriteBody(*headings);
    err|= WriteBody("</th>\r\n");
    headings++;
  }
  err|= WriteBody("</tr>\r\n");
  return err;
}

bool HTTPRequest::WriteTableRowStart()
{
  return WriteBody("  <tr>\r\n");
}

bool HTTPRequest::WriteTableRowEnd()
{
  return WriteBody("  </tr>\r\n");
}

bool HTTPRequest::WriteTableEnd()
{
  return WriteBody("</table>\r\n");
}

bool HTTPRequest::WriteTableColumn(long long value)
{
  return WriteBodyFmt("  <td>%lld</td>\r\n", value);
}

bool HTTPRequest::WriteTableColumn(unsigned long long value)
{
  return WriteBodyFmt("  <td>%llu</td>\r\n", value);
}

bool HTTPRequest::WriteTableColumn(long value)
{
  return WriteBodyFmt("  <td>%ld</td>\r\n", value);
}

bool HTTPRequest::WriteTableColumn(unsigned long value)
{
  return WriteBodyFmt("  <td>%lu</td>\r\n", value);
}

bool HTTPRequest::WriteTableColumn(const char *value)
{
  bool err= false;
  err|= WriteBody("  <td>");
  err|= WriteBody(value);
  err|= WriteBody("</td>\r\n");
  return err;
}

bool HTTPRequest::WriteTableColumn(const char *host, int port)
{
  return WriteBodyFmt("  <td>%s:%d</td>\r\n", host, port);
}

/**
  Write a formatted string for the HTTP response body text.
  Return true if an error occurred, false otherwise.
*/
bool HTTPRequest::WriteBodyFmt(const char *fmt, ...)
{
  char buff[1024];
  va_list ap;
  va_start(ap, fmt);
  int ret= vsnprintf(buff, sizeof(buff) - 1, fmt, ap);
  if (ret < 0)
    return true;
  va_end(ap);
  return WriteBody(buff, ret);
}

/**
  Allocate a two dimensional array of size rows*columns.  Memory
  is taken from the memory-pool.
  Return an array, or NULL if there were memory allocation problems.
*/
void **HTTPRequest::AllocArray(int rows, int columns)
{
  void **alloc;
  alloc= (void **)alloc_root(&req_mem_root,
                             (columns + 1) * sizeof(ulonglong *));
  if (alloc == NULL)
    return NULL;
  for (int col= 0; col < columns; col++)
  {
    alloc[col]= (void *)alloc_root(&req_mem_root,
                                   (rows + 1) * sizeof(ulonglong));
    if (alloc[col] == NULL)
      return NULL;
  }
  return alloc;
}

/**
  Generate a HTTP response header.  Set code to 200, if a successful
  response is being made, otherwise it defaults to 404.
*/
bool HTTPRequest::GenerateHeader(int code, bool html)
{
  char msg[128];
  char timebuf[32];
  struct timeval tv;

  gettimeofday(&tv, NULL);
  ctime_r(&tv.tv_sec, timebuf);

  if (code == 200)
    WriteHeader("HTTP/1.0 200 OK\r\n");
  else
    WriteHeader("HTTP/1.0 404 Not Found\r\n");

  if (html)
    WriteHeader("Content-Type: text/html; charset=UTF-8\r\n");
  else
    WriteHeader("Content-Type: text/plain; charset=UTF-8\r\n");

  WriteHeader("Server: mysqld\r\n");
  timebuf[strlen(timebuf) - 1]= '\0';         // Remove the terminating LF

  sprintf(msg, "Date: %s\r\n", timebuf);
  WriteHeader(msg);
  WriteHeader("Connection: Close\r\n");
  sprintf(msg, "ContentLength: %d\r\n\r\n", ResponseBodyLength());
  WriteHeader(msg);
  return false;
}

bool HTTPRequest::GenerateError(const char *msg)
{
  WriteBody(msg);
  return false;
}

/**
   Output a list of status variables that would otherwise be achieved
   by commands like 'SHOW VARIABLES' and 'SHOW STATUS'.

   Calling function must hold the lock on LOCK_status.

   @param[in]  prefix   prefix for the variables in #vars
   @param[in]  vars     an array of SHOW_VARs to be printed

   @return Operation status
     @retval  0       OK
     @retval  errno   ERROR
*/

int HTTPRequest::var_GenVars(const char *prefix, SHOW_VAR *vars)
{
  bool error= false;
  STATUS_VAR tmp_stat;
  SHOW_VAR *var;
  my_aligned_storage<SHOW_VAR_FUNC_BUFF_SIZE, MY_ALIGNOF(long)> buffer;
  char * const buff= buffer.data;

  calc_sum_of_all_status(&tmp_stat);

  if (prefix == NULL)
    prefix= "";

  for (SHOW_VAR *variables= vars; variables->name && !error; variables++)
  {
    char *result, *resultend;
    char varbuf[1024];
    SHOW_VAR tmp;

    /*
      if var->type is SHOW_FUNC, call the function.
      Repeat as necessary, if new var is again SHOW_FUNC
    */
    for (var=variables; var->type == SHOW_FUNC; var= &tmp)
      ((mysql_show_var_func)(var->value))(thd_, &tmp, buff);
    tmp= *var;

    if (tmp.type == SHOW_ARRAY)
    {
      char new_prefix[MAX_PREFIX_LENGTH];
      // +2, one for the '_' and the other for the \0
      char name[MAX_PREFIX_LENGTH * 2 + 2];

      // lowercase our prefix. Mostly useful for Com_ variables.
      strncpy(name, tmp.name, MAX_PREFIX_LENGTH);
      name[0]= (char) tolower((int) name[0]);

      strncpy(new_prefix, prefix, MAX_PREFIX_LENGTH);
      strncat(new_prefix, name, MAX_PREFIX_LENGTH);
      strncat(new_prefix, "_", MAX_PREFIX_LENGTH);
      var_GenVars(new_prefix, (SHOW_VAR *) tmp.value);
    }
    else
      get_variable_value(thd_, &tmp, OPT_GLOBAL, &tmp_stat,
                         varbuf, &result, &resultend);

    /*
      MySQL status variables are always capitalised on the first
      character, so we choose to lowercase it here to fit in with
      the existing format provided by the mysql_var_reporter.
    */
    error |= WriteBodyFmt("%s%c%s %s\r\n",
                          prefix, tolower(variables->name[0]),
                          variables->name + 1, result);
  }
  return (error) ? ER_OUT_OF_RESOURCES : 0;
}

/**
  Output the contents of SHOW STATUS to the http response.

  Handles locking of the status variables that is necessary
  for ::var_GenVars to function properly.

  @return  Operation Status
    @retval  0      OK
    @retval  errno  ERROR
*/

int HTTPRequest::var_ShowStatus()
{
  pthread_mutex_lock(&LOCK_status);
  /*
    The mysql_var_reporter was responsible for prepending
    variable names with 'status_'.
  */
  int ret= var_GenVars("status_", status_vars);
  pthread_mutex_unlock(&LOCK_status);
  return ret;
}


/**
   Output the contents of  "SHOW MASTER STATUS" to the http repsonse.

   @return  Operation Status
     @retval  0      OK
     @retval  errno  ERROR
 */

int HTTPRequest::var_MasterStatus()
{
  Master_info *mi= active_mi;
  bool err= false;
  ulonglong group_id;
  uint32 group_server_id;

  if (mi == NULL || mi->host[0] == '\0')
  {
    err|= WriteBodyFmt("master_configured 1\r\n");
    err|= WriteBodyFmt("slave_configured 0\r\n");
    return (err) ? ER_OUT_OF_RESOURCES : 0;
  }

  mysql_bin_log.get_group_and_server_id(&group_id, &group_server_id);

  err|= WriteBodyFmt("master_configured 0\r\n");
  err|= WriteBodyFmt("slave_configured 1\r\n");

  pthread_mutex_lock(&mi->data_lock);
  pthread_mutex_lock(&mi->rli.data_lock);
  in_addr ip;
  inet_pton(AF_INET, mi->host, &ip);
  err|= WriteBodyFmt("master_host %d\r\n", (uint32) ip.s_addr);
  err|= WriteBodyFmt("master_port %d\r\n", (uint32) mi->port);
  err|= WriteBodyFmt("connect_retry %d\r\n", (uint32) mi->connect_retry);
  err|= WriteBodyFmt("read_master_log_pos %lld\r\n", mi->master_log_pos);
  err|= WriteBodyFmt("read_master_log_name %s\r\n", mi->master_log_name);
  err|= WriteBodyFmt("relay_log_pos %lld\r\n", mi->rli.group_relay_log_pos);
  err|= WriteBodyFmt("relay_log_name %s\r\n", mi->rli.group_relay_log_name);
  err|= WriteBodyFmt("slave_io_running %d\r\n",
                     (mi->slave_running == MYSQL_SLAVE_RUN_CONNECT) ? 1 : 0);
  err|= WriteBodyFmt("slave_sql_running %d\r\n",
                     mi->rli.slave_running ? 1 : 0);
  pthread_mutex_lock(&mi->err_lock);
  pthread_mutex_lock(&mi->rli.err_lock);
  err |= WriteBodyFmt("last_sql_errno %d\r\n", (uint32) mi->rli.last_error().number);
  err |= WriteBodyFmt("last_io_errno %d\r\n", (uint32) mi->last_error().number);
  pthread_mutex_unlock(&mi->rli.err_lock);
  pthread_mutex_unlock(&mi->err_lock);
  err|= WriteBodyFmt("skip_counter %d\r\n",
                     (uint32) mi->rli.slave_skip_counter);
  err|= WriteBodyFmt("exec_master_log_pos %d\r\n",
                     (uint32) mi->rli.group_master_log_pos);
  err|= WriteBodyFmt("exec_master_log_name %s\r\n",
                     mi->rli.group_master_log_name);
  err|= WriteBodyFmt("relay_log_space %d\r\n",
                     (uint32) mi->rli.log_space_total);
  err|= WriteBodyFmt("until_log_pos %d\r\n",
                     (uint32) mi->rli.until_log_pos);
  err|= WriteBodyFmt("until_log_name %s\r\n",
                     mi->rli.until_log_name);
  err|= WriteBodyFmt("master_ssl_allowed %d\r\n",
                     (uint32) mi->ssl ? 1 : 0);
  err|= WriteBodyFmt("group_id %d\r\n",
                     (uint32) group_id);
  err|= WriteBodyFmt("group_server_id %d\r\n",
                     (uint32) group_server_id);

  long secs= 0;

  if (mi->slave_running == MYSQL_SLAVE_RUN_CONNECT && mi->rli.slave_running)
  {
    secs= (long)((time_t)time(NULL) - mi->rli.last_master_timestamp)
      - mi->clock_diff_with_master;

    if (secs < 0 || mi->rli.last_master_timestamp == 0)
      secs= 0;
  }

  err|= WriteBodyFmt("seconds_behind_master %lld\r\n", secs);
  pthread_mutex_unlock(&mi->rli.data_lock);
  pthread_mutex_unlock(&mi->data_lock);
  return (err) ? ER_OUT_OF_RESOURCES : 0;
}


/**
   Output the MySQL process table to the http response.

   @return  Operation Status
     @retval  0      OK
     @retval  errno  ERROR
 */

int HTTPRequest::var_ProcessList()
{
  int num_procs= 0;
  int idle_procs= 0;
  int active_procs= 0;
  long long int total_proc_time= 0;
  time_t now= time(NULL);
  time_t oldest_start_time= now;

  pthread_mutex_lock(&LOCK_thread_count);
  I_List_iterator<THD> it(threads);
  THD *tmp;
  while ((tmp= it++))
  {
    num_procs++;
    if (tmp->locked || tmp->net.reading_or_writing)
    {
      if (tmp->start_time < oldest_start_time)
        oldest_start_time= tmp->start_time;
      active_procs++;
    }
    else
    {
      idle_procs++;
    }
    total_proc_time+= now - tmp->start_time;
  }
  pthread_mutex_unlock(&LOCK_thread_count);

  long long int mean_proc_time= 0;
  if (num_procs > 0)
    mean_proc_time= total_proc_time / num_procs;
  bool err= false;
  err|= WriteBodyFmt("num_procs %d\r\n", num_procs);
  err|= WriteBodyFmt("idle_procs %d\r\n", idle_procs);
  err|= WriteBodyFmt("active_procs %d\r\n", active_procs);
  err|= WriteBodyFmt("mean_proc_time %lld\r\n", mean_proc_time);
  err|= WriteBodyFmt("total_proc_time %lld\r\n", total_proc_time);
  err|= WriteBodyFmt("oldest_active_proc_time %d\r\n", oldest_start_time);
  return (err) ? ER_OUT_OF_RESOURCES : 0;
}

/**
  Generate a process listing.
*/
int HTTPRequest::status_ProcessListing(time_t current_time)
{
  const char *headings[]= { "Id", "User", "Host", "Database", "Command",
                            "Time", "State", "Info", NULL };
  WriteTableHeader("Thread List", headings);
  pthread_mutex_lock(&LOCK_thread_count);
  I_List_iterator<THD> it(threads);
  THD *tmp;
  while ((tmp= it++))
  {
    Security_context *tmp_sctx= tmp->security_ctx;
    if (tmp->vio_ok() || tmp->system_thread)
    {
      WriteBodyFmt(" <tr>\r\n");
      WriteTableColumn(tmp->thread_id);
      WriteTableColumn((tmp_sctx->user) ? tmp_sctx->user :
                       (tmp->system_thread ?
                        "system user" : "unauthenticated user"));

      // Column Host
      if (tmp->peer_port && (tmp_sctx->host || tmp_sctx->ip)
          && thd_->security_ctx->host_or_ip[0])
        WriteTableColumn(tmp_sctx->host_or_ip, tmp->peer_port);
      else
        WriteTableColumn((tmp_sctx->host_or_ip[0]) ? tmp_sctx->host_or_ip
                         : (tmp_sctx->host) ? tmp_sctx->host : "");

      // Columns Database and Command
      WriteTableColumn((tmp->db) ? tmp->db : "");
      WriteTableColumn(command_name[tmp->command].str);

      // Column Time
      WriteTableColumn(current_time - tmp->start_time);

      // Column State
      if (tmp->killed == THD::KILL_CONNECTION)
        WriteTableColumn("Killed");
      else
      {
        if (tmp->mysys_var)
          pthread_mutex_lock(&tmp->mysys_var->mutex);
        WriteTableColumn((tmp->locked ? "Locked" :
                          tmp->net.reading_or_writing ?
                          (tmp->net.reading_or_writing == 2 ?
                           "Writing to net" :
                           tmp->command == COM_SLEEP ? ""
                           : "Reading from net") :
                          tmp->proc_info ? tmp->proc_info :
                          tmp->mysys_var &&
                          tmp->mysys_var->current_cond
                          ? "Waiting on cond" : ""));
        if (tmp->mysys_var)
          pthread_mutex_unlock(&tmp->mysys_var->mutex);
      }

      // Column Query
      WriteTableColumn((tmp->query()) ? tmp->query() : "");
      WriteBodyFmt(" </tr>\r\n");
    }
  }
  pthread_mutex_unlock(&LOCK_thread_count);
  WriteBodyFmt(" </table>\r\n");
  return 0;
}

/**
  Generate Master status
*/
int HTTPRequest::status_MasterStatus()
{
  Master_info *mi= active_mi;
  // Check if we are a slave server
  if (mi != NULL && mi->host[0] != '\0')
    return 0;

  if (mysql_bin_log.is_open())
  {
    const char *headings[] = { "File",
                               "Position",
                               "Group ID / Server ID",
                               NULL };
    WriteTableHeader("Master Status", headings);
    WriteTableRowStart();
    // Master server
    LOG_INFO li;
    mysql_bin_log.get_current_log(&li);
    WriteTableColumn(li.log_file_name);
    WriteTableColumn(li.pos);
    WriteBodyFmt("  <td>%lld / %lld</td>\r\n",
                 li.group_id, li.server_id);
    WriteTableRowEnd();
    WriteTableEnd();
  }
  return 0;
}

/**
   Prints SHOW SLAVE STATUS information to the http response.

   @return Operation Status
     @retval  0   OK
*/

int HTTPRequest::status_SlaveStatus()
{
  Master_info *mi = active_mi;
  ulonglong group_id;
  uint32 server_id;
  // Check if we are a master server.
  if (mi == NULL || mi->host[0] == '\0')
    return 0;
  const char *headings[] = { "Master Host", "User", "Connect Retries",
                             "Master Log File / Position",
                             "Relay Log File / Position",
                             "Relay Master Log File / Position",
                             "Group ID / Server ID",
                             "Slave IO / SQL",
                             "Seconds Behind",
                             NULL };
  WriteTableHeader("Slave Status", headings);
  WriteTableRowStart();
  mysql_bin_log.get_group_and_server_id(&group_id, &server_id);
  // Slave server
  pthread_mutex_lock(&mi->data_lock);
  pthread_mutex_lock(&mi->rli.data_lock);
  WriteTableColumn(mi->host, mi->port);
  WriteTableColumn(mi->user);
  WriteTableColumn((unsigned long) mi->connect_retry);
  WriteBodyFmt("  <td>%s / %lld</td>\r\n",
               (mi->master_log_name[0] == '\0')
               ? "&lt;empty>" : mi->master_log_name,
               mi->master_log_pos);
  WriteBodyFmt("  <td>%s / %lld</td>\r\n",
               (mi->rli.group_relay_log_name[0] == '\0')
               ? "&lt;empty>" : mi->rli.group_relay_log_name,
               mi->rli.group_relay_log_pos);
  WriteBodyFmt("  <td>%s / %lld</td>\r\n",
               (mi->rli.group_master_log_name[0] == '\0')
               ? "&lt;empty>" : mi->rli.group_master_log_name,
               mi->rli.group_master_log_pos);
  WriteBodyFmt("  <td>%lld / %lld</td>\r\n",
               group_id, server_id);

  // Columns Slave IO and Slave SQL
  WriteBodyFmt("  <td>%s / %s</td>\r\n",
               (mi->slave_running == MYSQL_SLAVE_RUN_CONNECT) ? "Yes" : "No",
               (mi->rli.slave_running) ? "Yes" : "No");
  long secs= 0;
  if (mi->slave_running == MYSQL_SLAVE_RUN_CONNECT && mi->rli.slave_running)
  {
    secs= (long)((time_t)time(NULL) - mi->rli.last_master_timestamp)
      - mi->clock_diff_with_master;
    if (secs < 0 || mi->rli.last_master_timestamp == 0)
      secs= 0;
  }
  // Column Seconds Behind
  WriteTableColumn(secs);

  pthread_mutex_unlock(&mi->rli.data_lock);
  pthread_mutex_unlock(&mi->data_lock);

  WriteTableRowEnd();
  WriteTableEnd();
  return 0;
}

/**
   Generate a response body for a /status URL.

   @return Operation Status
     @retval  0     OK
     @retval  errno ERROR
*/

int HTTPRequest::status(void)
{
  time_t current_time= time(0);

  WriteBody("<html><head>\r\n"
            "<meta HTTP-EQUIV=\"content-type\" "
            "CONTENT=\"text/html; charset=UTF-8\">\r\n");
  WriteBody("<title>Status for MySQL</title>\r\n</head>\r\n");
  WriteBody("<body bgcolor=#ffffff text=#000000>\r\n");
  WriteBodyFmt("<p><table bgcolor=#eeeeff width=100%>"
               "<tr align=center><td>"
               "<font size=+2>Status for MySQL %s %s</font>"
               "</td></tr></table></p>\r\n", server_version,
               MYSQL_COMPILATION_COMMENT);

  WriteBody("<table cellspacing=0 cellpadding=0 width=100%><tr>\r\n");
  {
    time_t diff= current_time - server_start_time;
    char start_time_str[64];
    ctime_r(&server_start_time, start_time_str);
    WriteBodyFmt("<td>Started: %s -- up %lld seconds<br>\r\n", start_time_str,
                 diff);
  }
  WriteBody("<br></td>\r\n");

  WriteBody("<td align=right valign=top>\r\n");
  WriteBodyFmt("Running on %s<br>\r\n", glob_hostname);
  WriteBody("</td></tr></table>\r\n");
  WriteBody("<p><a href=\"/var\">Local variables</a></p>");

  pthread_mutex_lock(&LOCK_server_started);
  if (mysqld_server_started)
  {
    pthread_mutex_unlock(&LOCK_server_started);
    status_ProcessListing(current_time);
    status_MasterStatus();
    status_SlaveStatus();
    // TODO: status_UserStats();
    // TODO: status_TableStats();
  }
  else
  {
    pthread_mutex_unlock(&LOCK_server_started);
    WriteBody("<p><font size=+2>MySQL is currently initialising</font></p>");
  }
  WriteBody("</body></html>");
  return 0;
}

/**
  Generate a response body for a /var URL.

  @return Operation Status
    @retval  0     OK
    @retval  errno ERROR
*/

int HTTPRequest::var(void)
{
  /*
    Test that the server is fully initialised.  Most of these
    statistics will fail until the table handlers have been
    initialised.
  */
  pthread_mutex_lock(&LOCK_server_started);
  if (!mysqld_server_started)
  {
    pthread_mutex_unlock(&LOCK_server_started);
    return 0;
  }
  pthread_mutex_unlock(&LOCK_server_started);

  int err= var_ShowStatus();

  //TODO: var_ShowwInnoDBStatus();
  //TODO: var_UserStats();
  //TODO: var_TableStats();

  if (!err)
    err= var_MasterStatus();
  if (!err)
    err= var_ProcessList();

  return err;
}

/**
  Generate a response body for a /health URL.


  @return Operation Status
    @retval  0     OK
    @retval  errno ERROR
*/

int HTTPRequest::health(void)
{
  WriteBody(STRING_WITH_LEN("OK\r\n"));
  return 0;
}

/**
  Generate a response to the /quitquitquit URL.
  This function will signal a clean shutdown of the current mysql instance.
  Note that this may kill active threads instead of waiting for pending
  transactions.
*/
int HTTPRequest::quitquitquit(void)
{
  sql_print_information("called quitquitquit");
  kill(getpid(), SIGQUIT);
  return 0;
}

/**
  Generate a response to the /abortabortabort URL.
  this function will terminate the mysql instance immediately, possibly
  leading to a dirty environment. Use /quitquitquit as an alternative
  when possible.
*/
int HTTPRequest::abortabortabort(void)
{
  sql_print_information("called abortabortabort");
  exit(1);
  return 0;
}
