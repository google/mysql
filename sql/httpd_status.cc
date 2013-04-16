// Copyright 2008 Google Inc. All Rights Reserved.

/*
  Implementation for the /status page.
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

/**
  Generate a process listing.
*/

int Http_request::status_process_list(time_t current_time)
{
  const char *headings[]=
  {
    "Id",
    "User",
    "Host",
    "Database",
    "Command",
    "Time",
    "State",
    "Info",
    NULL
  };

  pthread_mutex_lock(&LOCK_thread_count);
  I_List_iterator<THD> it(threads);
  THD *tmp;
  if (!threads.is_empty())
  {
    write_table_header("Thread List", headings);
  }
  while ((tmp= it++))
  {
    Security_context *tmp_sctx= tmp->security_ctx;
    if (tmp->vio_ok() || tmp->system_thread)
    {
      write_body_fmt(" <tr>\r\n");
      write_table_column(tmp->thread_id);
      write_table_column((tmp_sctx->user) ? tmp_sctx->user :
                         (tmp->system_thread ?
                          "system user" : "unauthenticated user"));

      /* Column Host. */
      if (tmp->peer_port && (tmp_sctx->host || tmp_sctx->ip)
          && thd_->security_ctx->host_or_ip[0])
        write_table_column(tmp_sctx->host_or_ip, tmp->peer_port);
      else
        write_table_column((tmp_sctx->host_or_ip[0]) ? tmp_sctx->host_or_ip
                           : (tmp_sctx->host) ? tmp_sctx->host : "");

      /* Columns Database and Command. */
      write_table_column((tmp->db) ? tmp->db : "");
      write_table_column(command_name[tmp->command].str);

      /* Column Time. */
      write_table_column(current_time - tmp->start_time);

      /* Column State. */
      if (tmp->killed == THD::KILL_CONNECTION)
        write_table_column("Killed");
      else
      {
        if (tmp->mysys_var)
          pthread_mutex_lock(&tmp->mysys_var->mutex);
        write_table_column((tmp->locked ? "Locked" :
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

      /* Column Query. */
      write_table_column((tmp->query()) ? tmp->query() : "");
      write_body_fmt(" </tr>\r\n");
    }
  }
  pthread_mutex_unlock(&LOCK_thread_count);
  write_body_fmt(" </table>\r\n");
  return 0;
}


/**
  Generate Master status.
*/

int Http_request::status_master_status()
{
  Master_info *mi= active_mi;
  /* Check if we are a slave server. */
  if (mi != NULL && mi->host[0] != '\0')
    return 0;

  if (mysql_bin_log.is_open())
  {
    const char *headings[]=
    {
      "File",
      "Position",
      "Group ID / Server ID",
      NULL
    };

    write_table_header("Master Status", headings);
    write_table_row_start();
    /* Master server. */
    LOG_INFO li;
    mysql_bin_log.get_current_log(&li);
    write_table_column(li.log_file_name);
    write_table_column(li.pos);
    write_body_fmt("  <td>%lld / %lld</td>\r\n",
                   li.group_id, li.server_id);
    write_table_row_end();
    write_table_end();
  }
  return 0;
}


/**
   Prints SHOW SLAVE STATUS information to the HTTP response.

   @return Operation Status
     @retval  0   OK
*/

int Http_request::status_slave_status()
{
  Master_info *mi= active_mi;
  ulonglong group_id;
  uint32 server_id;

  /* Check if we are a master server. */
  if (mi == NULL || mi->host[0] == '\0')
    return 0;

  const char *headings[]=
  {
    "Master Host",
    "User",
    "Connect Retries",
    "Master Log File / Position",
    "Relay Log File / Position",
    "Relay Master Log File / Position",
    "Group ID / Server ID",
    "Slave IO / SQL",
    "Seconds Behind",
    NULL
  };

  write_table_header("Slave Status", headings);
  write_table_row_start();
  mysql_bin_log.get_group_and_server_id(&group_id, &server_id);

  /* Slave server. */
  pthread_mutex_lock(&mi->data_lock);
  pthread_mutex_lock(&mi->rli.data_lock);
  write_table_column(mi->host, mi->port);
  write_table_column(mi->user);
  write_table_column((unsigned long) mi->connect_retry);
  write_body_fmt("  <td>%s / %lld</td>\r\n",
                 (mi->master_log_name[0] == '\0')
                 ? "&lt;empty>" : mi->master_log_name,
                 mi->master_log_pos);
  write_body_fmt("  <td>%s / %lld</td>\r\n",
                 (mi->rli.group_relay_log_name[0] == '\0')
                 ? "&lt;empty>" : mi->rli.group_relay_log_name,
                 mi->rli.group_relay_log_pos);
  write_body_fmt("  <td>%s / %lld</td>\r\n",
                 (mi->rli.group_master_log_name[0] == '\0')
                 ? "&lt;empty>" : mi->rli.group_master_log_name,
                 mi->rli.group_master_log_pos);
  write_body_fmt("  <td>%lld / %lld</td>\r\n",
                 group_id, server_id);

  /* Columns Slave IO and Slave SQL. */
  write_body_fmt("  <td>%s / %s</td>\r\n",
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

  /* Column Seconds Behind. */
  write_table_column(secs);

  pthread_mutex_unlock(&mi->rli.data_lock);
  pthread_mutex_unlock(&mi->data_lock);

  write_table_row_end();
  write_table_end();
  return 0;
}


/**
   Prints the contents of the INFORMATION_SCHEMA.USER_STATISTICS table
   to the http response.

   @return Operation Status
     @retval  0   OK
*/

int Http_request::status_user_statistics()
{
  pthread_mutex_lock(&LOCK_global_user_stats);

  if (global_user_stats.records > 0)
  {
    const char *headings[]=
    {
      "User",
      "Total Cxn",
      "Concurrent Cxn",
      "Connected Time",
      "Busy Time",
      "CPU Time",
      "Bytes RX",
      "Bytes TX",
      "Binlog Bytes Written",
      "Rows Fetched",
      "Rows Updated",
      "Table Rows Read",
      "Select Ops",
      "Update Ops",
      "Other Ops",
      "Commit TXN",
      "Rollback TXN",
      "Denied Cxn",
      "Lost Cxn",
      "Access Denied",
      "Empty Queries",
      NULL
    };

    write_table_header("User Statistics", headings);
    for (unsigned int i= 0; i < global_user_stats.records; ++i)
    {
      USER_STATS *u= (USER_STATS *) hash_element(&global_user_stats, i);

      write_table_row_start();
      write_table_column(u->user);
      write_table_column((unsigned long) u->total_connections);
      write_table_column((unsigned long) u->concurrent_connections);
      write_table_column(u->connected_time);
      write_table_column(u->busy_time);
      write_table_column(u->cpu_time);
      write_table_column(u->bytes_received);
      write_table_column(u->bytes_sent);
      write_table_column(u->binlog_bytes_written);
      write_table_column(u->rows_fetched);
      write_table_column(u->rows_updated);
      write_table_column(u->rows_read);
      write_table_column(u->select_commands);
      write_table_column(u->update_commands);
      write_table_column(u->other_commands);
      write_table_column(u->commit_trans);
      write_table_column(u->rollback_trans);
      write_table_column(u->denied_connections);
      write_table_column(u->lost_connections);
      write_table_column(u->access_denied_errors);
      write_table_column(u->empty_queries);
      write_table_row_end();
    }
    write_table_end();
  }

  pthread_mutex_unlock(&LOCK_global_user_stats);
  return 0;
}

/**
   Prints the contents of the INFORMATION_SCHEMA.TABLE_STATISTICS table
   to the http response.

   @return Operation Status
     @retval  0   OK
*/

int Http_request::status_table_statistics()
{
  pthread_mutex_lock(&LOCK_global_table_stats);

  if (global_table_stats.records > 0)
  {
    const char *headings[]=
    {
      "Table",
      "Rows Read",
      "Rows Changed",
      "Rows Changed * Indexes",
      NULL
    };

    write_table_header("Table Statistics", headings);
    for (unsigned int i= 0; i < global_table_stats.records; ++i)
    {
      TABLE_STATS *t= (TABLE_STATS *) hash_element(&global_table_stats, i);

      write_table_row_start();
      write_table_column(t->table);
      write_table_column(t->rows_read);
      write_table_column(t->rows_changed);
      write_table_column(t->rows_changed_x_indexes);
      write_table_row_end();
    }
    write_table_end();
  }

  pthread_mutex_unlock(&LOCK_global_table_stats);
  return 0;
}

/**
   Generate a response body for a /status URL.
*/

void Http_request::status(void)
{
  time_t current_time= time(0);

  write_body("<html><head>\r\n"
             "<meta HTTP-EQUIV=\"content-type\" "
             "CONTENT=\"text/html; charset=UTF-8\">\r\n");
  write_body("<title>Status for MySQL</title>\r\n</head>\r\n");
  write_body("<body bgcolor=#ffffff text=#000000>\r\n");
  write_body_fmt("<p><table bgcolor=#eeeeff width=100%>"
                 "<tr align=center><td>"
                 "<font size=+2>Status for MySQL %s %s</font>"
                 "</td></tr></table></p>\r\n", server_version,
                 MYSQL_COMPILATION_COMMENT);

  write_body("<table cellspacing=0 cellpadding=0 width=100%><tr>\r\n");
  {
    time_t diff= current_time - server_start_time;
    char start_time_str[64];
    ctime_r(&server_start_time, start_time_str);
    write_body_fmt("<td>Started: %s -- up %lld seconds<br>\r\n", start_time_str,
                   diff);
  }
  write_body("</td>\r\n");

  write_body("<td align=right valign=top>\r\n");
  write_body_fmt("Running on %s<br>\r\n", glob_hostname);
  write_body_fmt("View <a href=\"var\">variables</a><br>\r\n");
  write_body_fmt("View <a href=\"heap\">heap samples</a>, "
                 "<a href=\"growth\">heap growth</a>, "
                 "<a href=\"malloc\">malloc stats</a><br>\r\n");

  write_body("</td></tr></table>\r\n");

  pthread_mutex_lock(&LOCK_server_started);
  if (mysqld_server_started)
  {
    pthread_mutex_unlock(&LOCK_server_started);
    status_master_status();
    status_slave_status();
    status_process_list(current_time);
    status_user_statistics();
    status_table_statistics();
  }
  else
  {
    pthread_mutex_unlock(&LOCK_server_started);
    write_body("<p><font size=+2>MySQL is currently initialising</font></p>");
  }
  write_body("</body></html>");
}
