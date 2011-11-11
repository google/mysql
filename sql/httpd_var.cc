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
  var_print_var("%s %d\r\n", "group_id", (uint32) group_id);
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
  bool write= var_head ? false : true;
  LIST *node;

  if (var_head)
  {
    /* First argument is the key (a string). */
    va_start(ap, fmt);
    char* key= va_arg(ap, char*);
    va_end(ap);

    for(node= var_head; node != NULL; node= node->next)
    {
      if (!strcmp((char *) node->data, key))
      {
        write= true;
        break;
      }
    }
  }

  if (write)
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
  //TODO: var_user_statistics();
  //TODO: var_table_statistics();

  if (!err)
    err= var_master_status();
  if (!err)
    err= var_process_list();

  return err;
}
