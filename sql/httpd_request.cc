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
