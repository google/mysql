// Copyright 2008 Google Inc. All Rights Reserved.

/*
  Implementation of the /health, /qqq and /aaa pages and the common
  functions used to generate the HTML pages. The implementation of the
  /status is in httpd_status.cc while the /var is in httpd_var.cc.
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

bool Http_request::write_table_header(const char *title,
                                      const char *const *headings)
{
  bool err= false;
  err|= write_body("<p><table bgcolor=#eeeeff width=100%><tr align=center>"
                   "<td><font size=+2>");
  err|= write_body(title);
  err|= write_body("</font></td></tr></table></p>"
                   "<table bgcolor=#fff5ee>\r\n<tr bgcolor=#eee5de>\r\n");
  while (*headings && !err)
  {
    err|= write_body("  <th>");
    err|= write_body(*headings);
    err|= write_body("</th>\r\n");
    headings++;
  }
  err|= write_body("</tr>\r\n");
  return err;
}


bool Http_request::write_table_row_start()
{
  return write_body("  <tr>\r\n");
}


bool Http_request::write_table_row_end()
{
  return write_body("  </tr>\r\n");
}


bool Http_request::write_table_end()
{
  return write_body("</table>\r\n");
}


bool Http_request::write_table_column(long long value)
{
  return write_body_fmt("  <td>%lld</td>\r\n", value);
}


bool Http_request::write_table_column(unsigned long long value)
{
  return write_body_fmt("  <td>%llu</td>\r\n", value);
}


bool Http_request::write_table_column(long value)
{
  return write_body_fmt("  <td>%ld</td>\r\n", value);
}


bool Http_request::write_table_column(unsigned long value)
{
  return write_body_fmt("  <td>%lu</td>\r\n", value);
}


bool Http_request::write_table_column(double value)
{
  return write_body_fmt("  <td>%.3f</td>\r\n", value);
}


bool Http_request::write_table_column(const char *value)
{
  bool err= false;
  err|= write_body("  <td>");
  err|= write_body(value);
  err|= write_body("</td>\r\n");
  return err;
}


bool Http_request::write_table_column(const char *host, int port)
{
  return write_body_fmt("  <td>%s:%d</td>\r\n", host, port);
}


bool Http_request::write_body_fmt_va_list(const char *fmt, va_list ap)
{
  char buff[1024];
  int ret= vsnprintf(buff, sizeof(buff) - 1, fmt, ap);
  if (ret < 0)
    return true;
  return write_body(buff, ret);
}


/**
  Write a formatted string for the HTTP response body text.
  Return true if an error occurred, false otherwise.
*/

bool Http_request::write_body_fmt(const char *fmt, ...)
{
  va_list ap;
  bool res= false;
  va_start(ap, fmt);
  res= write_body_fmt_va_list(fmt, ap);
  va_end(ap);
  return res;
}


/**
  Allocate a two dimensional int array of size rows*columns.  Memory
  is taken from the memory-pool.

  @return an array or NULL if there were memory allocation problems
*/

void **Http_request::alloc_array(int rows, int columns)
{
  void **alloc;
  alloc= (void **) alloc_root(&req_mem_root,
                              (columns + 1) * sizeof(ulonglong *));

  if (alloc == NULL)
    return NULL;

  for (int col= 0; col < columns; col++)
  {
    alloc[col]= (void *) alloc_root(&req_mem_root,
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

bool Http_request::generate_header(int code, bool html)
{
  char msg[128];
  char timebuf[32];
  struct timeval tv;

  gettimeofday(&tv, NULL);
  ctime_r(&tv.tv_sec, timebuf);

  if (code == 200)
    write_header("HTTP/1.0 200 OK\r\n");
  else
    write_header("HTTP/1.0 404 Not Found\r\n");

  if (html)
    write_header("Content-Type: text/html; charset=UTF-8\r\n");
  else
    write_header("Content-Type: text/plain; charset=UTF-8\r\n");

  write_header("Server: mysqld\r\n");
  timebuf[strlen(timebuf) - 1]= '\0';         /* Remove the terminating LF. */

  sprintf(msg, "Date: %s\r\n", timebuf);
  write_header(msg);
  write_header("Connection: Close\r\n");
  sprintf(msg, "ContentLength: %d\r\n\r\n", response_body_length());
  write_header(msg);
  return false;
}


bool Http_request::generate_error(const char *msg)
{
  write_body(msg);
  return false;
}

/**
  Generate a response body for a /health URL.
*/

void Http_request::health(void)
{
  write_body(STRING_WITH_LEN("OK\r\n"));
}


/**
  Generate a response to the /quitquitquit URL.

  This function will signal a clean shutdown of the current mysql instance.
  Note that this may kill active threads instead of waiting for pending
  transactions.
*/

void Http_request::quitquitquit(void)
{
  sql_print_information("Called quitquitquit.");
  kill(getpid(), SIGQUIT);
}


/**
  Generate a response to the /abortabortabort URL.

  This function will terminate the mysql instance immediately, possibly
  leading to a dirty environment. Use /quitquitquit as an alternative
  when possible.
*/

void Http_request::abortabortabort(void)
{
  sql_print_information("Called abortabortabort.");
  exit(1);
}
