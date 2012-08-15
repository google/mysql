// Copyright 2008 Google Inc. All Rights Reserved.

/*
  Implementation for the /var page.
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

#define MAX_PREFIX_LENGTH 80                  /* Somewhat arbitrary. */
#define MAX_VAR_NAME_LENGTH 160

enum enum_user_statistics_array
{
  USER_STATS_TOTAL_CONNECTIONS= 0,
  USER_STATS_CONCURRENT_CONNECTIONS,
  USER_STATS_CONNECTED_TIME,
  USER_STATS_BUSY_TIME,
  USER_STATS_CPU_TIME,
  USER_STATS_BYTES_RECEIVED,
  USER_STATS_BYTES_SENT,
  USER_STATS_BINLOG_BYTES_WRITTEN,
  USER_STATS_ROWS_FETCHED,
  USER_STATS_ROWS_UPDATED,
  USER_STATS_TABLE_ROWS_READ,
  USER_STATS_SELECT_COMMANDS,
  USER_STATS_UPDATE_COMMANDS,
  USER_STATS_OTHER_COMMANDS,
  USER_STATS_COMMIT_TRANSACTIONS,
  USER_STATS_ROLLBACK_TRANSACTIONS,
  USER_STATS_DENIED_CONNECTIONS,
  USER_STATS_LOST_CONNECTIONS,
  USER_STATS_ACCESS_DENIED,
  USER_STATS_EMPTY_QUERIES,
  USER_STATS_NUM_PROCS_BY_USER,
  USER_STATS_ACTIVE_PROCS_BY_USER,
  USER_STATS_IDLE_PROCS_BY_USER,
  USER_STATS_TOTAL_ACTIVE_PROC_TIME,
  USER_STATS_END
};

enum enum_table_statistics_array
{
  TABLE_STATS_ROWS_READ= 0,
  TABLE_STATS_ROWS_CHANGED,
  TABLE_STATS_END
};

/**
   Insert the string spanned from *base to *end into 'var_head'.

   This is a helper function for parse_url_params. The string to insert
   begins at *base and extends to *end. A new list node is allocated to
   contain the new string. This node then becomes the new head of our
   linked list.
*/

void Http_request::insert_var(uchar *base, uchar *end)
{
  uchar *token;
  LIST *node;

  node= (LIST *) my_malloc(sizeof(LIST), MYF(0));
  token= (uchar *) my_malloc(sizeof(uchar) * ((end - base) + 1), MYF(0));
  strncpy((char *) token, (char *) base, end - base);
  token[end - base]= '\0';
  node->data= token;
  var_head= list_add(var_head, node);
}


/**
   Parse the /var parameter string and return the results.

   Currently the only supported format is:

     var=variable1:variable2:...

   Example URLs:

     http://localhost:8080/var?var=num_procs
     http://localhost:8080/var?var=num_procs:status_aborted_clients

   @param[in]      net  network object for this connection
   @param[in,out]  req  the Http_request object associated with this connection

   @return Operation Status
     @retval  true    ERROR
     @retval  false   OK
*/

bool Http_request::parse_url_params()
{
  /* Longest param list we accept. */
  int max_chars= 600;
  uchar *url= net->buff;
  /* Curent char we are inspecting. */
  uchar *c;
  /* Start of the current token. */
  uchar *base;
  const char *prefix= "/var?var=";

  /* Skip the "GET ". */
  url+= 4;

  /* Check to see whether or not we should expect at least one parameter. */
  if (strncmp((const char *) url, prefix, sizeof(prefix)))
  {
    return false;
  }

  while (*url != '=')
  {
    url++;
  }

  url++;
  base= url;

  for (int i= 0; i < max_chars; i++)
  {
    c= url + i;

    /* Perhaps this should be expanded at some point for more chars. */
    if (*c == ' ' || *c == '\n' || *c == '\r' || *c == '&')
    {
      insert_var(base, c);
      return false;
    }

    /* We may need a better rule at some point. */
    if (*c == ':')
    {
      insert_var(base, c);
      /* Skip the symbol we broke on. */
      c++;
      base= c;
    }
  }

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

int Http_request::var_gen_vars(const char *prefix, SHOW_VAR *vars)
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
      If var->type is SHOW_FUNC, call the function.
      Repeat as necessary, if new var is again SHOW_FUNC
    */
    for (var=variables; var->type == SHOW_FUNC; var= &tmp)
      ((mysql_show_var_func)(var->value))(thd_, &tmp, buff);
    tmp= *var;

    if (tmp.type == SHOW_ARRAY)
    {
      char new_prefix[MAX_PREFIX_LENGTH];
      /* +2, one for the '_' and the other for the \0. */
      char name[MAX_PREFIX_LENGTH * 2 + 2];

      /* Lowercase our prefix. Mostly useful for Com_ variables. */
      strncpy(name, tmp.name, MAX_PREFIX_LENGTH);
      name[0]= (char) tolower((int) name[0]);

      strncpy(new_prefix, prefix, MAX_PREFIX_LENGTH);
      strncat(new_prefix, name, MAX_PREFIX_LENGTH);
      strncat(new_prefix, "_", MAX_PREFIX_LENGTH);
      var_gen_vars(new_prefix, (SHOW_VAR *) tmp.value);
    }
    else
      get_variable_value(thd_, &tmp, OPT_GLOBAL, &tmp_stat,
                         varbuf, &result, &resultend);

    int key_length= strlen(prefix) + strlen(variables->name) + 1;
    char key[MAX_VAR_NAME_LENGTH];
    key_length= (key_length > MAX_VAR_NAME_LENGTH) ?
                MAX_VAR_NAME_LENGTH : key_length;

    /*
      MySQL status variables are always capitalised on the first
      character, so we choose to lowercase it here to fit in with
      the existing format provided by the mysql_var_reporter.
    */

    my_snprintf(key, key_length, "%s%c%s", prefix,
                tolower(variables->name[0]), variables->name + 1);

    var_print_var("%s %s\r\n", key, result);
  }
  return (error) ? ER_OUT_OF_RESOURCES : 0;
}


/**
  Output the contents of the INFORMATION_SCHEMA.USER_STATISTICS table.

  @return  Operation Status
    @retval  0                    OK
    @retval  ER_OUT_OF_RESOURCES
*/

int Http_request::var_user_statistics()
{
  pthread_mutex_lock(&LOCK_global_user_stats);
  int num_records= global_user_stats.records;

  if (num_records == 0)
  {
    /*
      No user statistics information.  This can happen when we've started
      mysqld and queried /var prior to any client connection.
    */
    pthread_mutex_unlock(&LOCK_global_user_stats);
    return 0;
  }

  /*
    Use a memory pool for allocations here, to avoid a potentially large
    stack allocation here.  There's no need to free this array.
  */
  char **user=
      (char **) alloc_root(&req_mem_root, num_records * sizeof(char *));
  ulonglong **stats= (ulonglong **) alloc_array(num_records, USER_STATS_END);

  if (user == NULL || stats == NULL)
  {
    pthread_mutex_unlock(&LOCK_global_user_stats);
    return ER_OUT_OF_RESOURCES;
  }

  /*
    We have to build the statistics into an array because the output
    is diametrically opposite to the input structure.
  */
  for (int i= 0; i < num_records; i++)
  {
    USER_STATS *us= (USER_STATS *) hash_element(&global_user_stats, i);
    stats[USER_STATS_TOTAL_CONNECTIONS][i]= us->total_connections;
    stats[USER_STATS_CONCURRENT_CONNECTIONS][i]= us->concurrent_connections;
    stats[USER_STATS_CONNECTED_TIME][i]= us->connected_time;
    stats[USER_STATS_BUSY_TIME][i]= (ulonglong)us->busy_time;
    stats[USER_STATS_CPU_TIME][i]= (ulonglong)us->cpu_time;
    stats[USER_STATS_BYTES_RECEIVED][i]= us->bytes_received;
    stats[USER_STATS_BYTES_SENT][i]= us->bytes_sent;
    stats[USER_STATS_BINLOG_BYTES_WRITTEN][i]= us->binlog_bytes_written;
    stats[USER_STATS_ROWS_FETCHED][i]= us->rows_fetched;
    stats[USER_STATS_ROWS_UPDATED][i]= us->rows_updated;
    stats[USER_STATS_TABLE_ROWS_READ][i]= us->rows_read;
    stats[USER_STATS_SELECT_COMMANDS][i]= us->select_commands;
    stats[USER_STATS_UPDATE_COMMANDS][i]= us->update_commands;
    stats[USER_STATS_OTHER_COMMANDS][i]= us->other_commands;
    stats[USER_STATS_COMMIT_TRANSACTIONS][i]= us->commit_trans;
    stats[USER_STATS_ROLLBACK_TRANSACTIONS][i]= us->rollback_trans;
    stats[USER_STATS_DENIED_CONNECTIONS][i]= us->denied_connections;
    stats[USER_STATS_LOST_CONNECTIONS][i]= us->lost_connections;
    stats[USER_STATS_ACCESS_DENIED][i]= us->access_denied_errors;
    stats[USER_STATS_EMPTY_QUERIES][i]= us->empty_queries;
    stats[USER_STATS_NUM_PROCS_BY_USER][i]= 0;
    stats[USER_STATS_ACTIVE_PROCS_BY_USER][i]= 0;
    stats[USER_STATS_IDLE_PROCS_BY_USER][i]= 0;
    stats[USER_STATS_TOTAL_ACTIVE_PROC_TIME][i]= 0;
    user[i]= strdup_root(&req_mem_root, us->user);
  }

  pthread_mutex_unlock(&LOCK_global_user_stats);

  /* Walk the process list build the user/process counts. */
  pthread_mutex_lock(&LOCK_thread_count);

  I_List_iterator<THD> it(threads);
  THD *tmp;
  time_t now= time(0);

  while ((tmp= it++))
  {
    for (int i= 0; i < num_records; i++)
    {
      if (tmp->security_ctx->user
          && strcmp(user[i], tmp->security_ctx->user) == 0)
      {
        stats[USER_STATS_NUM_PROCS_BY_USER][i]+= 1;
        if (tmp->locked || tmp->net.reading_or_writing)
          stats[USER_STATS_ACTIVE_PROCS_BY_USER][i]+= 1;
        else
          stats[USER_STATS_IDLE_PROCS_BY_USER][i]+= 1;
        stats[USER_STATS_TOTAL_ACTIVE_PROC_TIME][i]+= now - tmp->start_time;
      }
    }
  }
  pthread_mutex_unlock(&LOCK_thread_count);

  const char *header[]=
  {
    "connections_by_user",
    "concurrent_connections_by_user",
    "connected_time_by_user",
    "busy_time_by_user",
    "cpu_time_by_user",
    "bytes_received_by_user",
    "bytes_sent_by_user",
    "binlog_bytes_written_by_user",
    "rows_fetched_by_user",
    "rows_updated_by_user",
    "rows_read_by_user",
    "select_commands_by_user",
    "update_commands_by_user",
    "other_commands_by_user",
    "commit_trans_by_user",
    "rollback_trans_by_user",
    "denied_connections_by_user",
    "lost_connections_by_user",
    "access_denied_errors_by_user",
    "empty_queries_by_user",
    "num_procs_by_user",
    "idle_procs_by_user",
    "active_procs_by_user",
    "total_active_proc_time_by_user"
  };

  assert(sizeof(header) / sizeof(char *) == USER_STATS_END);

  bool error= false;
  for (int i= 0; i < USER_STATS_END && !error; i++)
    if (var_need_print_var(header[i]))
    {
      error|= write_body(header[i]);
      error|= write_body(" \"map:user");
      for (int j= 0; j < num_records && !error; j++)
        error|= write_body_fmt(" %s:%llu", user[j], stats[i][j]);
      error|= write_body(STRING_WITH_LEN("\"\r\n"));
    }

  return (error) ? ER_OUT_OF_RESOURCES : 0;
}


/**
  Output the contents of the INFORMATION_SCHEMA.TABLE_STATISTICS table.

  @return  Operation Status
    @retval  0      OK
    @retval  ER_OUT_OF_RESOURCES
*/

int Http_request::var_table_statistics()
{
  pthread_mutex_lock(&LOCK_global_table_stats);
  int num_records= global_table_stats.records;

  if (num_records == 0)
  {
    /*
      This shouldn't happen, but just in case, guard against there
      being no tables on the server.
    */
    pthread_mutex_unlock(&LOCK_global_table_stats);
    return 0;
  }

  char **table=
      (char **) alloc_root(&req_mem_root, num_records * sizeof(char *));
  ulonglong **rows= (ulonglong **) alloc_array(num_records, TABLE_STATS_END);

  if (table == NULL || rows == NULL)
  {
    pthread_mutex_unlock(&LOCK_global_table_stats);
    return ER_OUT_OF_RESOURCES;
  }

  for (int i= 0; i < num_records; i++)
  {
    TABLE_STATS *t= (TABLE_STATS *) hash_element(&global_table_stats, i);
    table[i]= strdup_root(&req_mem_root, t->table);
    rows[TABLE_STATS_ROWS_READ][i]= t->rows_read;
    rows[TABLE_STATS_ROWS_CHANGED][i]= t->rows_changed;
  }

  pthread_mutex_unlock(&LOCK_global_table_stats);

  const char *header[]=
  {
    "rows_read_by_table",
    "rows_changed_by_table"
  };

  assert(sizeof(header) / sizeof(char *) == TABLE_STATS_END);

  bool error= false;
  for (int i= 0; i < TABLE_STATS_END && !error; i++)
    if (var_need_print_var(header[i]))
    {
      error|= write_body(header[i]);
      error|= write_body(" \"map:db.table");
      for (int j= 0; j < num_records && !error; j++)
        error|= write_body_fmt(" %s:%llu", table[j], rows[i][j]);
      error|= write_body(STRING_WITH_LEN("\"\r\n"));
    }

  return (error) ? ER_OUT_OF_RESOURCES : 0;
}


/**
  Output the contents of SHOW STATUS to the HTTP response.

  Handles locking of the status variables that is necessary
  for ::var_gen_vars to function properly.

  @return  Operation Status
    @retval  0      OK
    @retval  errno  ERROR
*/

int Http_request::var_show_status()
{
  pthread_mutex_lock(&LOCK_status);
  /*
    The mysql_var_reporter was responsible for prepending
    variable names with 'status_'.
  */
  int ret= var_gen_vars("status_", status_vars);
  pthread_mutex_unlock(&LOCK_status);
  return ret;
}


/**
   Output the contents of  "SHOW MASTER STATUS" to the HTTP repsonse.

   @return  Operation Status
     @retval  0      OK
     @retval  errno  ERROR
 */

int Http_request::var_master_status()
{
  Master_info *mi= active_mi;
  bool err= false;
  ulonglong group_id;
  uint32 group_server_id;

  if (mi == NULL || mi->host[0] == '\0')
  {
    var_print_var("%s %s\r\n", "master_configured", "1");
    var_print_var("%s %s\r\n", "slave_configured", "0");
    return (err) ? ER_OUT_OF_RESOURCES : 0;
  }

  mysql_bin_log.get_group_and_server_id(&group_id, &group_server_id);

  var_print_var("%s %s\r\n", "master_configured", "1");
  var_print_var("%s %s\r\n", "slave_configured", "0");

  pthread_mutex_lock(&mi->data_lock);
  pthread_mutex_lock(&mi->rli.data_lock);
  in_addr ip;
  inet_pton(AF_INET, mi->host, &ip);
  var_print_var("%s %d\r\n", "master_host", (uint32) ip.s_addr);
  var_print_var("%s %d\r\n", "master_port ", (uint32) mi->port);
  var_print_var("%s %d\r\n", "connect_retry", (uint32) mi->connect_retry);
  var_print_var("%s %d\r\n", "read_master_log_pos", mi->master_log_pos);
  var_print_var("%s %s\r\n", "read_master_log_name", mi->master_log_name);
  var_print_var("%s %d\r\n", "relay_log_pos", mi->rli.group_relay_log_pos);
  var_print_var("%s %s\r\n", "relay_log_name", mi->rli.group_relay_log_name);
  var_print_var("%s %d\r\n", "slave_io_running",
                 (mi->slave_running == MYSQL_SLAVE_RUN_CONNECT) ? 1 : 0);
  var_print_var("%s %d\r\n", "slave_sql_running",
                 mi->rli.slave_running ? 1 : 0);
  pthread_mutex_lock(&mi->err_lock);
  pthread_mutex_lock(&mi->rli.err_lock);
  var_print_var("%s %d\r\n", "last_sql_errno",
                 (uint32) mi->rli.last_error().number);
  var_print_var("%s %d\r\n", "last_io_errno",
                 (uint32) mi->last_error().number);
  pthread_mutex_unlock(&mi->rli.err_lock);
  pthread_mutex_unlock(&mi->err_lock);
  var_print_var("%s %d\r\n", "skip_counter",
                 (uint32) mi->rli.slave_skip_counter);
  var_print_var("%s %d\r\n", "exec_master_log_pos",
                 (uint32) mi->rli.group_master_log_pos);
  var_print_var("%s %s\r\n", "exec_master_log_name",
                 mi->rli.group_master_log_name);
  var_print_var("%s %d\r\n", "relay_log_space",
                 (uint32) mi->rli.log_space_total);
  var_print_var("%s %d\r\n", "until_log_pos", (uint32) mi->rli.until_log_pos);
  var_print_var("%s %s\r\n", "until_log_name",  mi->rli.until_log_name);
  var_print_var("%s %d\r\n", "master_ssl_allowed", (uint32) mi->ssl ? 1 : 0);
  var_print_var("%s %llu\r\n", "group_id", group_id);
  var_print_var("%s %d\r\n", "group_server_id", (uint32) group_server_id);

  long secs= 0;

  if (mi->slave_running == MYSQL_SLAVE_RUN_CONNECT && mi->rli.slave_running)
  {
    secs= (long)((time_t)time(NULL) - mi->rli.last_master_timestamp)
      - mi->clock_diff_with_master;

    if (secs < 0 || mi->rli.last_master_timestamp == 0)
      secs= 0;
  }

  var_print_var("%s %d\r\n", "seconds_behind_master", secs);
  pthread_mutex_unlock(&mi->rli.data_lock);
  pthread_mutex_unlock(&mi->data_lock);
  return (err) ? ER_OUT_OF_RESOURCES : 0;
}


/**
   Output the MySQL process table to the HTTP response.

   @return  Operation Status
     @retval  0      OK
     @retval  errno  ERROR
 */

int Http_request::var_process_list()
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
  var_print_var("%s %d\r\n", "num_procs", num_procs);
  var_print_var("%s %d\r\n", "idle_procs", idle_procs);
  var_print_var("%s %d\r\n", "active_procs", active_procs);
  var_print_var("%s %lld\r\n", "mean_proc_time", mean_proc_time);
  var_print_var("%s %lld\r\n", "total_proc_time", total_proc_time);
  var_print_var("%s %d\r\n", "oldest_active_proc_time", oldest_start_time);
  return (err) ? ER_OUT_OF_RESOURCES : 0;
}


/**
   Determines if the current filtering is selecting a given variable.

   @param [in]  var  variable to check
   @return
     @retval  true   var needs to be printed
     @retval  false  var doesn't need to be printed
*/

bool Http_request::var_need_print_var(const char *var)
{
  LIST *node;

  if (var_head)
  {
    for(node= var_head; node != NULL; node= node->next)
    {
      if (!strcmp((char *) node->data, var))
        return true;
    }
  }

  return var_head ? false : true;
}


/**
   Prints out the given variable, filtering it if the url query asks
   for specific variables and the current one is not one of them.

   @return Operation status
     @retval  0       OK
     @retval  errno   ERROR
*/

int Http_request::var_print_var(const char *fmt, ...)
{
  va_list ap;
  bool error;

  va_start(ap, fmt);
  char* key= va_arg(ap, char*);
  va_end(ap);

  if (var_need_print_var(key))
  {
    va_start(ap, fmt);
    error= write_body_fmt_va_list(fmt, ap);
    va_end(ap);
  }

  return error;
}


/**
  Generate a response body for a /var URL.

  @return Operation Status
    @retval  0      OK
    @retval  errno  ERROR
*/

int Http_request::var(void)
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

  int err= var_show_status();

  //TODO: var_show_innodb_status();
  var_user_statistics();
  var_table_statistics();

  if (!err)
    err= var_master_status();
  if (!err)
    err= var_process_list();

  return err;
}
