/* Copyright (c) 2000, 2011, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */


/**
  @file

  @brief
  logging of commands

  @todo
    Abort logging when we get an error in reading or writing log files
*/

#include "mysql_priv.h"
#include "sql_repl.h"
#include "rpl_filter.h"
#include "rpl_rli.h"
#include "repl_semi_sync.h"

#include <my_dir.h>
#include <stdarg.h>
#include <m_ctype.h>				// For test_if_number

#ifdef __NT__
#include "message.h"
#endif

#include "repl_hier_cache.h"

#include <mysql/plugin.h>

/* max size of the log message */
#define MAX_LOG_BUFFER_SIZE 1024
#define MAX_USER_HOST_SIZE 512
#define MAX_TIME_SIZE 32
#define MY_OFF_T_UNDEF (~(my_off_t)0UL)

#define FLAGSTR(V,F) ((V)&(F)?#F" ":"")

LOGGER logger;

MYSQL_BIN_LOG mysql_bin_log;
ulong sync_binlog_counter= 0;

MYSQL_SQL_LOG mysql_sql_log;

/*
  Set true if logging is to be enabled for all tables, whereby all
  checks against individual tables is disabled.
*/
static bool audit_log_all_tables;

/*
  Pointer to a copy of the parameter given to --audit_log_tables with
  comma-separated-tablenames replaced with null-separated-tablenames.  The
  audit_log_tables hash table makes references into this.
*/
static char *audit_log_table_list= NULL;

/*
  Keep track of the state of our audit_log hash table.
*/
static bool is_audit_hashtable_inited= false;

/*
   Table that we want to force a log of queries on
*/
static HASH audit_log_tables;

/*
  A flag array of SQL commands that should be filtered.
  If an element is marked TRUE, then the SQL command will be blocked.
  If an element is marked FALSE (the default), then the SQL command will
  be logged.
*/
static bool audit_stmt_filter[SQLCOM_END + 1];

const char rpl_hierarchical_index_delimiter= '|';

static bool test_if_number(const char *str,
			   long *res, bool allow_wildcards);
static int binlog_init(void *p);
static int binlog_close_connection(handlerton *hton, THD *thd);
static int binlog_savepoint_set(handlerton *hton, THD *thd, void *sv);
static int binlog_savepoint_rollback(handlerton *hton, THD *thd, void *sv);
static int binlog_commit(handlerton *hton, THD *thd, bool all);
static int binlog_rollback(handlerton *hton, THD *thd, bool all);
static int binlog_prepare(handlerton *hton, THD *thd, bool all);

enum_sqllog_stmt_type sql_command2sqllog_type(enum_sql_command sql_command);

/* Queries with the correct log position in the event */
struct QueryLogEvent
{
  const char *query_;
  const uint32 query_length_;
};

QueryLogEvent query_with_log[]=
{
  { "BEGIN", static_cast<uint32>(strlen("BEGIN")) },
  { "COMMIT", static_cast<uint32>(strlen("COMMIT")) }
};

/**
  Silence all errors and warnings reported when performing a write
  to a log table.
  Errors and warnings are not reported to the client or SQL exception
  handlers, so that the presence of logging does not interfere and affect
  the logic of an application.
*/
class Silence_log_table_errors : public Internal_error_handler
{
  char m_message[MYSQL_ERRMSG_SIZE];
public:
  Silence_log_table_errors()
  {
    m_message[0]= '\0';
  }

  virtual ~Silence_log_table_errors() {}

  virtual bool handle_error(uint sql_errno, const char *message,
                            MYSQL_ERROR::enum_warning_level level,
                            THD *thd);
  const char *message() const { return m_message; }
};

bool
Silence_log_table_errors::handle_error(uint /* sql_errno */,
                                       const char *message_arg,
                                       MYSQL_ERROR::enum_warning_level /* level */,
                                       THD * /* thd */)
{
  strmake(m_message, message_arg, sizeof(m_message)-1);
  return TRUE;
}


sql_print_message_func sql_print_message_handlers[3] =
{
  sql_print_information,
  sql_print_warning,
  sql_print_error
};


char *make_default_log_name(char *buff,const char* log_ext)
{
  strmake(buff, pidfile_name, FN_REFLEN-5);
  return fn_format(buff, buff, mysql_data_home, log_ext,
                   MYF(MY_UNPACK_FILENAME|MY_REPLACE_EXT));
}

/*
  Helper class to hold a mutex for the duration of the
  block.

  Eliminates the need for explicit unlocking of mutexes on, e.g.,
  error returns.  On passing a null pointer, the sentry will not do
  anything.
 */
class Mutex_sentry
{
public:
  Mutex_sentry(pthread_mutex_t *mutex)
    : m_mutex(mutex)
  {
    if (m_mutex)
      pthread_mutex_lock(mutex);
  }

  ~Mutex_sentry()
  {
    if (m_mutex)
      pthread_mutex_unlock(m_mutex);
#ifndef DBUG_OFF
    m_mutex= 0;
#endif
  }

private:
  pthread_mutex_t *m_mutex;

  // It's not allowed to copy this object in any way
  Mutex_sentry(Mutex_sentry const&);
  void operator=(Mutex_sentry const&);
};

/*
  Helper class to store binary log transaction data.
*/
class binlog_trx_data {
public:
  binlog_trx_data()
    : at_least_one_stmt_committed(0), incident(FALSE), m_pending(0),
    before_stmt_pos(MY_OFF_T_UNDEF)
  {
    trans_log.end_of_file= max_binlog_cache_size;
  }

  ~binlog_trx_data()
  {
    DBUG_ASSERT(pending() == NULL);
    close_cached_file(&trans_log);
  }

  my_off_t position() const {
    return my_b_tell(&trans_log);
  }

  bool empty() const
  {
    return pending() == NULL && my_b_tell(&trans_log) == 0;
  }

  /*
    Truncate the transaction cache to a certain position. This
    includes deleting the pending event.
   */
  void truncate(my_off_t pos)
  {
    DBUG_PRINT("info", ("truncating to position %lu", (ulong) pos));
    DBUG_PRINT("info", ("before_stmt_pos=%lu", (ulong) pos));
    if (pending())
    {
      delete pending();
    }
    set_pending(0);
    reinit_io_cache(&trans_log, WRITE_CACHE, pos, 0, 0);
    trans_log.end_of_file= max_binlog_cache_size;
    if (pos < before_stmt_pos)
      before_stmt_pos= MY_OFF_T_UNDEF;

    /*
      The only valid positions that can be truncated to are at the
      beginning of a statement. We are relying on this fact to be able
      to set the at_least_one_stmt_committed flag correctly. In other word, if
      we are truncating to the beginning of the transaction cache,
      there will be no statements in the cache, otherwhise, we will
      have at least one statement in the transaction cache.
     */
    at_least_one_stmt_committed= (pos > 0);
  }

  /*
    Reset the entire contents of the transaction cache, emptying it
    completely.
   */
  void reset() {
    if (!empty())
      truncate(0);
    before_stmt_pos= MY_OFF_T_UNDEF;
    incident= FALSE;
    trans_log.end_of_file= max_binlog_cache_size;
    DBUG_ASSERT(empty());
  }

  Rows_log_event *pending() const
  {
    return m_pending;
  }

  void set_pending(Rows_log_event *const pending)
  {
    m_pending= pending;
  }

  IO_CACHE trans_log;                         // The transaction cache

  void set_incident(void)
  {
    incident= TRUE;
  }
  
  bool has_incident(void)
  {
    return(incident);
  }

  /**
    Boolean that is true if there is at least one statement in the
    transaction cache.
  */
  bool at_least_one_stmt_committed;
  bool incident;

private:
  /*
    Pending binrows event. This event is the event where the rows are
    currently written.
   */
  Rows_log_event *m_pending;

public:
  /*
    Binlog position before the start of the current statement.
  */
  my_off_t before_stmt_pos;
};

handlerton *binlog_hton;

bool LOGGER::is_log_table_enabled(uint log_table_type)
{
  switch (log_table_type) {
  case QUERY_LOG_SLOW:
    return (table_log_handler != NULL) && opt_slow_log;
  case QUERY_LOG_GENERAL:
    return (table_log_handler != NULL) && opt_log ;
  case QUERY_LOG_AUDIT:
    return (table_log_handler != NULL) && opt_audit_log;
  default:
    DBUG_ASSERT(0);
    return FALSE;                             /* make compiler happy */
  }
}


/* Check if a given table is opened log table */
int check_if_log_table(uint db_len, const char *db, uint table_name_len,
                       const char *table_name, uint check_if_opened)
{
  if (db_len == 5 &&
      !(lower_case_table_names ?
        my_strcasecmp(system_charset_info, db, "mysql") :
        strcmp(db, "mysql")))
  {
    if (table_name_len == 11 && !(lower_case_table_names ?
                                  my_strcasecmp(system_charset_info,
                                                table_name, "general_log") :
                                  strcmp(table_name, "general_log")))
    {
      if (!check_if_opened || logger.is_log_table_enabled(QUERY_LOG_GENERAL))
        return QUERY_LOG_GENERAL;
      return 0;
    }

    if (table_name_len == 8 && !(lower_case_table_names ?
      my_strcasecmp(system_charset_info, table_name, "slow_log") :
      strcmp(table_name, "slow_log")))
    {
      if (!check_if_opened || logger.is_log_table_enabled(QUERY_LOG_SLOW))
        return QUERY_LOG_SLOW;
      return 0;
    }
  }
  return 0;
}


Log_to_csv_event_handler::Log_to_csv_event_handler()
{
}


Log_to_csv_event_handler::~Log_to_csv_event_handler()
{
}


void Log_to_csv_event_handler::cleanup()
{
  logger.is_log_tables_initialized= FALSE;
}

/* log event handlers */

/**
  Log command to the general log table

  Log given command to the general log table.

  @param  event_time        command start timestamp
  @param  user_host         the pointer to the string with user@host info
  @param  user_host_len     length of the user_host string. this is computed
                            once and passed to all general log event handlers
  @param  thread_id         Id of the thread, issued a query
  @param  command_type      the type of the command being logged
  @param  command_type_len  the length of the string above
  @param  sql_text          the very text of the query being executed
  @param  sql_text_len      the length of sql_text string


  @return This function attempts to never call my_error(). This is
  necessary, because general logging happens already after a statement
  status has been sent to the client, so the client can not see the
  error anyway. Besides, the error is not related to the statement
  being executed and is internal, and thus should be handled
  internally (@todo: how?).
  If a write to the table has failed, the function attempts to
  write to a short error message to the file. The failure is also
  indicated in the return value. 

  @retval  FALSE   OK
  @retval  TRUE    error occured
*/

bool Log_to_csv_event_handler::
  log_general(THD *thd, time_t event_time, const char *user_host,
              uint user_host_len, int thread_id,
              const char *command_type, uint command_type_len,
              const char *sql_text, uint sql_text_len,
              CHARSET_INFO *client_cs)
{
  TABLE_LIST table_list;
  TABLE *table;
  bool result= TRUE;
  bool need_close= FALSE;
  bool need_pop= FALSE;
  bool need_rnd_end= FALSE;
  uint field_index;
  Silence_log_table_errors error_handler;
  Open_tables_state open_tables_backup;
  ulonglong save_thd_options;
  bool save_time_zone_used;

  /*
    CSV uses TIME_to_timestamp() internally if table needs to be repaired
    which will set thd->time_zone_used
  */
  save_time_zone_used= thd->time_zone_used;

  save_thd_options= thd->options;
  thd->options&= ~OPTION_BIN_LOG;

  bzero(& table_list, sizeof(TABLE_LIST));
  table_list.alias= table_list.table_name= GENERAL_LOG_NAME.str;
  table_list.table_name_length= GENERAL_LOG_NAME.length;

  table_list.lock_type= TL_WRITE_CONCURRENT_INSERT;

  table_list.db= MYSQL_SCHEMA_NAME.str;
  table_list.db_length= MYSQL_SCHEMA_NAME.length;

  /*
    1) open_performance_schema_table generates an error of the
    table can not be opened or is corrupted.
    2) "INSERT INTO general_log" can generate warning sometimes.

    Suppress these warnings and errors, they can't be dealt with
    properly anyway.

    QQ: this problem needs to be studied in more detail.
    Comment this 2 lines and run "cast.test" to see what's happening.
  */
  thd->push_internal_handler(& error_handler);
  need_pop= TRUE;

  if (!(table= open_performance_schema_table(thd, & table_list,
                                             & open_tables_backup)))
    goto err;

  need_close= TRUE;

  if (table->file->extra(HA_EXTRA_MARK_AS_LOG_TABLE) ||
      table->file->ha_rnd_init(0))
    goto err;

  need_rnd_end= TRUE;

  /* Honor next number columns if present */
  table->next_number_field= table->found_next_number_field;

  /*
    NOTE: we do not call restore_record() here, as all fields are
    filled by the Logger (=> no need to load default ones).
  */

  /*
    We do not set a value for table->field[0], as it will use
    default value (which is CURRENT_TIMESTAMP).
  */

  /* check that all columns exist */
  if (table->s->fields < 6)
    goto err;

  DBUG_ASSERT(table->field[0]->type() == MYSQL_TYPE_TIMESTAMP);

  ((Field_timestamp*) table->field[0])->store_timestamp((my_time_t)
                                                        event_time);

  /* do a write */
  if (table->field[1]->store(user_host, user_host_len, client_cs) ||
      table->field[2]->store((longlong) thread_id, TRUE) ||
      table->field[3]->store((longlong) server_id, TRUE) ||
      table->field[4]->store(command_type, command_type_len, client_cs))
    goto err;

  /*
    A positive return value in store() means truncation.
    Still logging a message in the log in this case.
  */
  table->field[5]->flags|= FIELDFLAG_HEX_ESCAPE;
  if (table->field[5]->store(sql_text, sql_text_len, client_cs) < 0)
    goto err;

  /* mark all fields as not null */
  table->field[1]->set_notnull();
  table->field[2]->set_notnull();
  table->field[3]->set_notnull();
  table->field[4]->set_notnull();
  table->field[5]->set_notnull();

  /* Set any extra columns to their default values */
  for (field_index= 6 ; field_index < table->s->fields ; field_index++)
  {
    table->field[field_index]->set_default();
  }

  /* log table entries are not replicated */
  if (table->file->ha_write_row(table->record[0]))
    goto err;

  result= FALSE;

err:
  if (result && !thd->killed)
    sql_print_error("Failed to write to mysql.general_log: %s",
                    error_handler.message());

  if (need_rnd_end)
  {
    table->file->ha_rnd_end();
    table->file->ha_release_auto_increment();
  }
  if (need_pop)
    thd->pop_internal_handler();
  if (need_close)
    close_performance_schema_table(thd, & open_tables_backup);

  thd->options= save_thd_options;
  thd->time_zone_used= save_time_zone_used;
  return result;
}



bool Log_to_csv_event_handler::
  log_audit(THD *thd, time_t event_time, const char *user_host,
              uint user_host_len, int thread_id,
              const char *command_type, uint command_type_len,
              const char *sql_text, uint sql_text_len,
              CHARSET_INFO *client_cs)
{
  /*
    Filler function at the moment, due to the fact that log_audit is a
    virtual function in Log_event_handler, and thus must be
    implemented to avoid errors.
  */
  sql_print_warning("Logging the audit log to CSV was attempted. "
                    "This is not a supported operation.");
  return false;
}

/*
  Log a query to the slow log table

  SYNOPSIS
    log_slow()
    thd               THD of the query
    current_time      current timestamp
    query_start_arg   command start timestamp
    user_host         the pointer to the string with user@host info
    user_host_len     length of the user_host string. this is computed once
                      and passed to all general log event handlers
    query_time        Amount of time the query took to execute (in microseconds)
    lock_time         Amount of time the query was locked (in microseconds)
    is_command        The flag, which determines, whether the sql_text is a
                      query or an administrator command (these are treated
                      differently by the old logging routines)
    sql_text          the very text of the query or administrator command
                      processed
    sql_text_len      the length of sql_text string

  DESCRIPTION

   Log a query to the slow log table

  RETURN
    FALSE - OK
    TRUE - error occured
*/

bool Log_to_csv_event_handler::
  log_slow(THD *thd, time_t current_time, time_t query_start_arg,
           const char *user_host, uint user_host_len,
           ulonglong query_utime, ulonglong lock_utime, bool is_command,
           const char *sql_text, uint sql_text_len)
{
  TABLE_LIST table_list;
  TABLE *table;
  bool result= TRUE;
  bool need_close= FALSE;
  bool need_rnd_end= FALSE;
  Silence_log_table_errors error_handler;
  Open_tables_state open_tables_backup;
  CHARSET_INFO *client_cs= thd->variables.character_set_client;
  bool save_time_zone_used;
  DBUG_ENTER("Log_to_csv_event_handler::log_slow");

  thd->push_internal_handler(& error_handler);
  /*
    CSV uses TIME_to_timestamp() internally if table needs to be repaired
    which will set thd->time_zone_used
  */
  save_time_zone_used= thd->time_zone_used;

  bzero(& table_list, sizeof(TABLE_LIST));
  table_list.alias= table_list.table_name= SLOW_LOG_NAME.str;
  table_list.table_name_length= SLOW_LOG_NAME.length;

  table_list.lock_type= TL_WRITE_CONCURRENT_INSERT;

  table_list.db= MYSQL_SCHEMA_NAME.str;
  table_list.db_length= MYSQL_SCHEMA_NAME.length;

  if (!(table= open_performance_schema_table(thd, & table_list,
                                             & open_tables_backup)))
    goto err;

  need_close= TRUE;

  if (table->file->extra(HA_EXTRA_MARK_AS_LOG_TABLE) ||
      table->file->ha_rnd_init(0))
    goto err;

  need_rnd_end= TRUE;

  /* Honor next number columns if present */
  table->next_number_field= table->found_next_number_field;

  restore_record(table, s->default_values);    // Get empty record

  /* check that all columns exist */
  if (table->s->fields < 11)
    goto err;

  /* store the time and user values */
  DBUG_ASSERT(table->field[0]->type() == MYSQL_TYPE_TIMESTAMP);
  ((Field_timestamp*) table->field[0])->store_timestamp((my_time_t)
                                                        current_time);
  if (table->field[1]->store(user_host, user_host_len, client_cs))
    goto err;

  if (query_start_arg)
  {
    longlong query_time= (longlong) (query_utime/1000000);
    longlong lock_time=  (longlong) (lock_utime/1000000);
    /*
      A TIME field can not hold the full longlong range; query_time or
      lock_time may be truncated without warning here, if greater than
      839 hours (~35 days)
    */
    MYSQL_TIME t;
    t.neg= 0;

    /* fill in query_time field */
    calc_time_from_sec(&t, (long) min(query_time, (longlong) TIME_MAX_VALUE_SECONDS), 0);
    if (table->field[2]->store_time(&t, MYSQL_TIMESTAMP_TIME))
      goto err;
    /* lock_time */
    calc_time_from_sec(&t, (long) min(lock_time, (longlong) TIME_MAX_VALUE_SECONDS), 0);
    if (table->field[3]->store_time(&t, MYSQL_TIMESTAMP_TIME))
      goto err;
    /* rows_sent */
    if (table->field[4]->store((longlong) thd->sent_row_count, TRUE))
      goto err;
    /* rows_examined */
    if (table->field[5]->store((longlong) thd->examined_row_count, TRUE))
      goto err;
  }
  else
  {
    table->field[2]->set_null();
    table->field[3]->set_null();
    table->field[4]->set_null();
    table->field[5]->set_null();
  }
  /* fill database field */
  if (thd->db)
  {
    if (table->field[6]->store(thd->db, thd->db_length, client_cs))
      goto err;
    table->field[6]->set_notnull();
  }

  if (thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt)
  {
    if (table->
        field[7]->store((longlong)
                        thd->first_successful_insert_id_in_prev_stmt_for_binlog,
                        TRUE))
      goto err;
    table->field[7]->set_notnull();
  }

  /*
    Set value if we do an insert on autoincrement column. Note that for
    some engines (those for which get_auto_increment() does not leave a
    table lock until the statement ends), this is just the first value and
    the next ones used may not be contiguous to it.
  */
  if (thd->auto_inc_intervals_in_cur_stmt_for_binlog.nb_elements() > 0)
  {
    if (table->
        field[8]->store((longlong)
          thd->auto_inc_intervals_in_cur_stmt_for_binlog.minimum(), TRUE))
      goto err;
    table->field[8]->set_notnull();
  }

  if (table->field[9]->store((longlong) server_id, TRUE))
    goto err;
  table->field[9]->set_notnull();

  /*
    Column sql_text.
    A positive return value in store() means truncation.
    Still logging a message in the log in this case.
  */
  if (table->field[10]->store(sql_text, sql_text_len, client_cs) < 0)
    goto err;

  /* log table entries are not replicated */
  if (table->file->ha_write_row(table->record[0]))
    goto err;

  result= FALSE;

err:
  thd->pop_internal_handler();

  if (result && !thd->killed)
    sql_print_error("Failed to write to mysql.slow_log: %s",
                    error_handler.message());

  if (need_rnd_end)
  {
    table->file->ha_rnd_end();
    table->file->ha_release_auto_increment();
  }
  if (need_close)
    close_performance_schema_table(thd, & open_tables_backup);
  thd->time_zone_used= save_time_zone_used;
  DBUG_RETURN(result);
}

int Log_to_csv_event_handler::
  activate_log(THD *thd, uint log_table_type)
{
  TABLE_LIST table_list;
  TABLE *table;
  int result;
  Open_tables_state open_tables_backup;

  DBUG_ENTER("Log_to_csv_event_handler::activate_log");

  bzero(& table_list, sizeof(TABLE_LIST));

  if (log_table_type == QUERY_LOG_GENERAL)
  {
    table_list.alias= table_list.table_name= GENERAL_LOG_NAME.str;
    table_list.table_name_length= GENERAL_LOG_NAME.length;
  }
  else
  {
    DBUG_ASSERT(log_table_type == QUERY_LOG_SLOW);
    table_list.alias= table_list.table_name= SLOW_LOG_NAME.str;
    table_list.table_name_length= SLOW_LOG_NAME.length;
  }

  table_list.lock_type= TL_WRITE_CONCURRENT_INSERT;

  table_list.db= MYSQL_SCHEMA_NAME.str;
  table_list.db_length= MYSQL_SCHEMA_NAME.length;

  table= open_performance_schema_table(thd, & table_list,
                                       & open_tables_backup);
  if (table)
  {
    result= 0;
    close_performance_schema_table(thd, & open_tables_backup);
  }
  else
    result= 1;

  DBUG_RETURN(result);
}

bool Log_to_csv_event_handler::
  log_error(enum loglevel level, const char *format, va_list args)
{
  /* No log table is implemented */
  DBUG_ASSERT(0);
  return FALSE;
}

bool Log_to_file_event_handler::
  log_error(enum loglevel level, const char *format,
            va_list args)
{
  return vprint_msg_to_log(level, format, args);
}

void Log_to_file_event_handler::init_pthread_objects()
{
  mysql_log.init_pthread_objects();
  mysql_slow_log.init_pthread_objects();
  mysql_audit_log.init_pthread_objects();
}


/** Wrapper around MYSQL_LOG::write() for slow log. */

bool Log_to_file_event_handler::
  log_slow(THD *thd, time_t current_time, time_t query_start_arg,
           const char *user_host, uint user_host_len,
           ulonglong query_utime, ulonglong lock_utime, bool is_command,
           const char *sql_text, uint sql_text_len)
{
  Silence_log_table_errors error_handler;
  thd->push_internal_handler(&error_handler);
  bool retval= mysql_slow_log.write(thd, current_time, query_start_arg,
                                    user_host, user_host_len,
                                    query_utime, lock_utime, is_command,
                                    sql_text, sql_text_len);
  thd->pop_internal_handler();
  return retval;
}


/**
   Wrapper around MYSQL_LOG::write() for general log. We need it since we
   want all log event handlers to have the same signature.
*/

bool Log_to_file_event_handler::
  log_general(THD *thd, time_t event_time, const char *user_host,
              uint user_host_len, int thread_id,
              const char *command_type, uint command_type_len,
              const char *sql_text, uint sql_text_len,
              CHARSET_INFO *client_cs)
{
  Silence_log_table_errors error_handler;
  thd->push_internal_handler(&error_handler);
  bool retval= mysql_log.write(event_time, user_host, user_host_len,
                               thread_id, command_type, command_type_len,
                               sql_text, sql_text_len);
  thd->pop_internal_handler();
  return retval;
}

/**
   Wrapper around MYSQL_LOG::write() for audit log.
*/

bool Log_to_file_event_handler::
  log_audit(THD *thd, time_t event_time, const char *user_host,
            uint user_host_len, int thread_id,
            const char *command_type, uint command_type_len,
            const char *sql_text, uint sql_text_len,
            CHARSET_INFO *client_cs)
{
  Silence_log_table_errors error_handler;
  thd->push_internal_handler(&error_handler);
  bool retval= mysql_audit_log.write(event_time, user_host, user_host_len,
                               thread_id, command_type, command_type_len,
                               sql_text, sql_text_len);
  thd->pop_internal_handler();
  return retval;
}


bool Log_to_file_event_handler::init()
{
  if (!is_initialized)
  {
    if (opt_slow_log)
      mysql_slow_log.open_slow_log(sys_var_slow_log_path.value);

    if (opt_log)
      mysql_log.open_query_log(sys_var_general_log_path.value);

    if (opt_audit_log)
      mysql_audit_log.open_audit_log(sys_var_audit_log_path.value);

    is_initialized= TRUE;
  }

  return FALSE;
}


void Log_to_file_event_handler::cleanup()
{
  mysql_log.cleanup();
  mysql_slow_log.cleanup();
  mysql_audit_log.cleanup();
}

void Log_to_file_event_handler::flush()
{
  /* reopen log files */
  if (opt_log)
    mysql_log.reopen_file();
  if (opt_slow_log)
    mysql_slow_log.reopen_file();
  if (opt_audit_log)
    mysql_audit_log.reopen_file();
}

/*
  Log error with all enabled log event handlers

  SYNOPSIS
    error_log_print()

    level             The level of the error significance: NOTE,
                      WARNING or ERROR.
    format            format string for the error message
    args              list of arguments for the format string

  RETURN
    FALSE - OK
    TRUE - error occured
*/

bool LOGGER::error_log_print(enum loglevel level, const char *format,
                             va_list args)
{
  bool error= FALSE;
  Log_event_handler **current_handler;

  /* currently we don't need locking here as there is no error_log table */
  for (current_handler= error_log_handler_list ; *current_handler ;)
    error= (*current_handler++)->log_error(level, format, args) || error;

  return error;
}

const uchar *audit_log_tables_get_key(const char *table,
                                      size_t *length,
                                      my_bool not_used __attribute__((unused)))
{
  *length= strlen(table);
  return (uchar*) table;
}

void init_audit_logging(void)
{
  // Default to turning off audit logging.
  audit_log_all_tables= false;
  /*
    Initialise statement filters to open logging for all.  These are
    selectively turned off by command line options.
  */
  for (int x= 0; x <= SQLCOM_END; x++)
    audit_stmt_filter[x]= false;
}

void free_audit_logging(void)
{
  if (is_audit_hashtable_inited)
    hash_free(&audit_log_tables);
  if (audit_log_table_list)
    free(audit_log_table_list);
}

void LOGGER::cleanup_base()
{
  DBUG_ASSERT(inited == 1);
  rwlock_destroy(&LOCK_logger);
  if (table_log_handler)
  {
    table_log_handler->cleanup();
    delete table_log_handler;
    table_log_handler= NULL;
  }
  if (file_log_handler)
    file_log_handler->cleanup();

  free_audit_logging();
}


void LOGGER::cleanup_end()
{
  DBUG_ASSERT(inited == 1);
  if (file_log_handler)
  {
    delete file_log_handler;
    file_log_handler=NULL;
  }
  inited= 0;
}


/**
  Perform basic log initialization: create file-based log handler and
  init error log.
*/
void LOGGER::init_base()
{
  DBUG_ASSERT(inited == 0);
  inited= 1;

  /*
    Here we initialize all of our audit log settings to the "off" position.
    This works becase we have not het handled the command line parameters
    that may set these values.
  */
  init_audit_logging();

  /*
    Here we create file log handler. We don't do it for the table log handler
    here as it cannot be created so early. The reason is THD initialization,
    which depends on the system variables (parsed later).
  */
  if (!file_log_handler)
    file_log_handler= new Log_to_file_event_handler;

  /* by default we use traditional error log */
  init_error_log(LOG_FILE);

  file_log_handler->init_pthread_objects();
  my_rwlock_init(&LOCK_logger, NULL);
}


void LOGGER::init_log_tables()
{
  if (!table_log_handler)
    table_log_handler= new Log_to_csv_event_handler;

  if (!is_log_tables_initialized &&
      !table_log_handler->init() && !file_log_handler->init())
    is_log_tables_initialized= TRUE;
}


bool LOGGER::flush_logs(THD *thd)
{
  int rc= 0;

  /*
    Now we lock logger, as nobody should be able to use logging routines while
    log tables are closed
  */
  logger.lock_exclusive();

  /* reopen log files */
  file_log_handler->flush();

  /* end of log flush */
  logger.unlock();
  return rc;
}


/*
  Log slow query with all enabled log event handlers

  SYNOPSIS
    slow_log_print()

    thd                 THD of the query being logged
    query               The query being logged
    query_length        The length of the query string
    current_utime       Current time in microseconds (from undefined start)

  RETURN
    FALSE   OK
    TRUE    error occured
*/

bool LOGGER::slow_log_print(THD *thd, const char *query, uint query_length,
                            ulonglong current_utime)

{
  bool error= FALSE;
  Log_event_handler **current_handler;
  bool is_command= FALSE;
  char user_host_buff[MAX_USER_HOST_SIZE + 1];
  Security_context *sctx= thd->security_ctx;
  uint user_host_len= 0;
  ulonglong query_utime, lock_utime;

  DBUG_ASSERT(thd->enable_slow_log);
  /*
    Print the message to the buffer if we have slow log enabled
  */

  if (*slow_log_handler_list)
  {
    time_t current_time;

    /* do not log slow queries from replication threads */
    if (thd->slave_thread && !opt_log_slow_slave_statements)
      return 0;

    lock_shared();
    if (!opt_slow_log)
    {
      unlock();
      return 0;
    }

    /* fill in user_host value: the format is "%s[%s] @ %s [%s]" */
    user_host_len= (strxnmov(user_host_buff, MAX_USER_HOST_SIZE,
                             sctx->priv_user ? sctx->priv_user : "", "[",
                             sctx->user ? sctx->user : "", "] @ ",
                             sctx->host ? sctx->host : "", " [",
                             sctx->ip ? sctx->ip : "", "]", NullS) -
                    user_host_buff);

    current_time= my_time_possible_from_micro(current_utime);
    if (thd->start_utime)
    {
      query_utime= (current_utime - thd->start_utime);
      lock_utime=  (thd->utime_after_lock - thd->start_utime);
    }
    else
    {
      query_utime= lock_utime= 0;
    }

    if (!query)
    {
      is_command= TRUE;
      query= command_name[thd->command].str;
      query_length= command_name[thd->command].length;
    }

    for (current_handler= slow_log_handler_list; *current_handler ;)
      error= (*current_handler++)->log_slow(thd, current_time, thd->start_time,
                                            user_host_buff, user_host_len,
                                            query_utime, lock_utime, is_command,
                                            query, query_length) || error;

    unlock();
  }
  return error;
}

bool LOGGER::general_log_write(THD *thd, enum enum_server_command command,
                               const char *query, uint query_length)
{
  bool error= FALSE;
  Log_event_handler **current_handler= general_log_handler_list;
  char user_host_buff[MAX_USER_HOST_SIZE + 1];
  Security_context *sctx= thd->security_ctx;
  uint user_host_len= 0;
  time_t current_time;

  DBUG_ASSERT(thd);

  lock_shared();
  if (!opt_log)
  {
    unlock();
    return 0;
  }
  user_host_len= strxnmov(user_host_buff, MAX_USER_HOST_SIZE,
                          sctx->priv_user ? sctx->priv_user : "", "[",
                          sctx->user ? sctx->user : "", "] @ ",
                          sctx->host ? sctx->host : "", " [",
                          sctx->ip ? sctx->ip : "", "]", NullS) -
                                                          user_host_buff;

  current_time= my_time(0);
  while (*current_handler)
    error|= (*current_handler++)->
      log_general(thd, current_time, user_host_buff,
                  user_host_len, thd->thread_id,
                  command_name[(uint) command].str,
                  command_name[(uint) command].length,
                  query, query_length,
                  thd->variables.character_set_client) || error;
  unlock();

  return error;
}

bool LOGGER::general_log_print(THD *thd, enum enum_server_command command,
                               const char *format, va_list args)
{
  uint message_buff_len= 0;
  char message_buff[MAX_LOG_BUFFER_SIZE];

  /* prepare message */
  if (format)
    message_buff_len= my_vsnprintf(message_buff, sizeof(message_buff),
                                   format, args);
  else
    message_buff[0]= '\0';

  return general_log_write(thd, command, message_buff, message_buff_len);
}

bool LOGGER::audit_log_write(THD *thd, enum enum_server_command command,
                               const char *query, uint query_length)
{
  bool error= FALSE;
  Log_event_handler **current_handler= audit_log_handler_list;
  char user_host_buff[MAX_USER_HOST_SIZE + 1];
  Security_context *sctx= thd->security_ctx;
  uint user_host_len= 0;
  time_t current_time;

  DBUG_ASSERT(thd);

  lock_shared();
  if (!opt_audit_log)
  {
    unlock();
    return 0;
  }
  user_host_len= strxnmov(user_host_buff, MAX_USER_HOST_SIZE,
                          sctx->priv_user ? sctx->priv_user : "", "[",
                          sctx->user ? sctx->user : "", "] @ ",
                          sctx->host ? sctx->host : "", " [",
                          sctx->ip ? sctx->ip : "", "]", NullS) -
                                                          user_host_buff;

  current_time= my_time(0);
  while (*current_handler)
    error|= (*current_handler++)->
      log_audit(thd, current_time, user_host_buff,
                user_host_len, thd->thread_id,
                command_name[(uint) command].str,
                command_name[(uint) command].length,
                query, query_length,
                thd->variables.character_set_client) || error;
  unlock();

  return error;
}

bool LOGGER::audit_log_print(THD *thd, enum enum_server_command command,
                             const char *format, va_list args)
{
  uint message_buff_len= 0;
  char message_buff[MAX_LOG_BUFFER_SIZE];

  /* prepare message */
  if (format)
    message_buff_len= my_vsnprintf(message_buff, sizeof(message_buff),
                                   format, args);
  else
    message_buff[0]= '\0';

  return audit_log_write(thd, command, message_buff, message_buff_len);
}

void LOGGER::init_error_log(uint error_log_printer)
{
  if (error_log_printer & LOG_NONE)
  {
    error_log_handler_list[0]= 0;
    return;
  }

  switch (error_log_printer) {
  case LOG_FILE:
    error_log_handler_list[0]= file_log_handler;
    error_log_handler_list[1]= 0;
    break;
    /* these two are disabled for now */
  case LOG_TABLE:
    DBUG_ASSERT(0);
    break;
  case LOG_TABLE|LOG_FILE:
    DBUG_ASSERT(0);
    break;
  }
}

void LOGGER::init_slow_log(uint slow_log_printer)
{
  if (slow_log_printer & LOG_NONE)
  {
    slow_log_handler_list[0]= 0;
    return;
  }

  switch (slow_log_printer) {
  case LOG_FILE:
    slow_log_handler_list[0]= file_log_handler;
    slow_log_handler_list[1]= 0;
    break;
  case LOG_TABLE:
    slow_log_handler_list[0]= table_log_handler;
    slow_log_handler_list[1]= 0;
    break;
  case LOG_TABLE|LOG_FILE:
    slow_log_handler_list[0]= file_log_handler;
    slow_log_handler_list[1]= table_log_handler;
    slow_log_handler_list[2]= 0;
    break;
  }
}

void LOGGER::init_general_log(uint general_log_printer)
{
  if (general_log_printer & LOG_NONE)
  {
    general_log_handler_list[0]= 0;
    return;
  }

  switch (general_log_printer) {
  case LOG_FILE:
    general_log_handler_list[0]= file_log_handler;
    general_log_handler_list[1]= 0;
    break;
  case LOG_TABLE:
    general_log_handler_list[0]= table_log_handler;
    general_log_handler_list[1]= 0;
    break;
  case LOG_TABLE|LOG_FILE:
    general_log_handler_list[0]= file_log_handler;
    general_log_handler_list[1]= table_log_handler;
    general_log_handler_list[2]= 0;
    break;
  }
}

void LOGGER::init_audit_log(uint audit_log_printer)
{
  if (audit_log_printer & LOG_NONE)
  {
    audit_log_handler_list[0]= 0;
    return;
  }

  switch (audit_log_printer) {
  case LOG_FILE:
    audit_log_handler_list[0]= file_log_handler;
    audit_log_handler_list[1]= 0;
    break;
  case LOG_TABLE:
    audit_log_handler_list[0]= table_log_handler;
    audit_log_handler_list[1]= 0;
    break;
  case LOG_TABLE|LOG_FILE:
    audit_log_handler_list[0]= file_log_handler;
    audit_log_handler_list[1]= table_log_handler;
    audit_log_handler_list[2]= 0;
    break;
  }
}

bool LOGGER::activate_log_handler(THD* thd, uint log_type)
{
  MYSQL_QUERY_LOG *file_log;
  bool res= FALSE;
  lock_exclusive();
  switch (log_type) {
  case QUERY_LOG_SLOW:
    if (!opt_slow_log)
    {
      file_log= file_log_handler->get_mysql_slow_log();

      file_log->open_slow_log(sys_var_slow_log_path.value);
      if (table_log_handler->activate_log(thd, QUERY_LOG_SLOW))
      {
        /* Error printed by open table in activate_log() */
        res= TRUE;
        file_log->close(0);
      }
      else
      {
        init_slow_log(log_output_options);
        opt_slow_log= TRUE;
      }
    }
    break;
  case QUERY_LOG_GENERAL:
    if (!opt_log)
    {
      file_log= file_log_handler->get_mysql_log();

      file_log->open_query_log(sys_var_general_log_path.value);
      if (table_log_handler->activate_log(thd, QUERY_LOG_GENERAL))
      {
        /* Error printed by open table in activate_log() */
        res= TRUE;
        file_log->close(0);
      }
      else
      {
        init_general_log(log_output_options);
        opt_log= TRUE;
      }
    }
    break;
  case QUERY_LOG_AUDIT:
    if (!opt_audit_log)
    {
      file_log= file_log_handler->get_mysql_audit_log();

      file_log->open_query_log(sys_var_audit_log_path.value);
      if (table_log_handler->activate_log(thd, QUERY_LOG_AUDIT))
      {
        /* Error printed by open table in activate_log() */
        res= TRUE;
        file_log->close(0);
      }
      else
      {
        init_audit_log(log_output_options);
        opt_audit_log= TRUE;
      }
    }
    break;
  default:
    DBUG_ASSERT(0);
  }
  unlock();
  return res;
}


void LOGGER::deactivate_log_handler(THD *thd, uint log_type)
{
  my_bool *tmp_opt= 0;
  MYSQL_LOG *file_log;

  switch (log_type) {
  case QUERY_LOG_SLOW:
    tmp_opt= &opt_slow_log;
    file_log= file_log_handler->get_mysql_slow_log();
    break;
  case QUERY_LOG_GENERAL:
    tmp_opt= &opt_log;
    file_log= file_log_handler->get_mysql_log();
    break;
  case QUERY_LOG_AUDIT:
    tmp_opt= &opt_audit_log;
    file_log= file_log_handler->get_mysql_audit_log();
    break;
  default:
    MY_ASSERT_UNREACHABLE();
  }

  if (!(*tmp_opt))
    return;

  lock_exclusive();
  file_log->close(0);
  *tmp_opt= FALSE;
  unlock();
}


/* the parameters are unused for the log tables */
bool Log_to_csv_event_handler::init()
{
  return 0;
}

int LOGGER::set_handlers(uint error_log_printer,
                         uint slow_log_printer,
                         uint general_log_printer,
                         uint audit_log_printer)
{
  /* error log table is not supported yet */
  DBUG_ASSERT(error_log_printer < LOG_TABLE);

  lock_exclusive();

  if ((slow_log_printer & LOG_TABLE || general_log_printer & LOG_TABLE
       || audit_log_printer & LOG_TABLE) &&
      !is_log_tables_initialized)
  {
    slow_log_printer= (slow_log_printer & ~LOG_TABLE) | LOG_FILE;
    general_log_printer= (general_log_printer & ~LOG_TABLE) | LOG_FILE;
    audit_log_printer= (audit_log_printer & ~LOG_TABLE) | LOG_FILE;
    sql_print_error("Failed to initialize log tables. "
                    "Falling back to the old-fashioned logs");
  }

  init_error_log(error_log_printer);
  init_slow_log(slow_log_printer);
  init_general_log(general_log_printer);
  init_audit_log(audit_log_printer);

  unlock();

  return 0;
}

/** 
    This function checks if a transactional talbe was updated by the
    current statement.

    @param thd The client thread that executed the current statement.
    @return
      @c true if a transactional table was updated, @false otherwise.
*/
static bool stmt_has_updated_trans_table(THD *thd)
{
  Ha_trx_info *ha_info;

  for (ha_info= thd->transaction.stmt.ha_list; ha_info && ha_info->is_started(); ha_info= ha_info->next())
  {
    if (ha_info->is_trx_read_write() && ha_info->ht() != binlog_hton)
      return (TRUE);
  }
  return (FALSE);
}

 /*
  Save position of binary log transaction cache.

  SYNPOSIS
    binlog_trans_log_savepos()

    thd      The thread to take the binlog data from
    pos      Pointer to variable where the position will be stored

  DESCRIPTION

    Save the current position in the binary log transaction cache into
    the variable pointed to by 'pos'
 */

static void
binlog_trans_log_savepos(THD *thd, my_off_t *pos)
{
  DBUG_ENTER("binlog_trans_log_savepos");
  DBUG_ASSERT(pos != NULL);
  if (thd_get_ha_data(thd, binlog_hton) == NULL)
    thd->binlog_setup_trx_data();
  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);
  DBUG_ASSERT(mysql_bin_log.is_open());
  *pos= trx_data->position();
  DBUG_PRINT("return", ("*pos: %lu", (ulong) *pos));
  DBUG_VOID_RETURN;
}


/*
  Truncate the binary log transaction cache.

  SYNPOSIS
    binlog_trans_log_truncate()

    thd      The thread to take the binlog data from
    pos      Position to truncate to

  DESCRIPTION

    Truncate the binary log to the given position. Will not change
    anything else.

 */
static void
binlog_trans_log_truncate(THD *thd, my_off_t pos)
{
  DBUG_ENTER("binlog_trans_log_truncate");
  DBUG_PRINT("enter", ("pos: %lu", (ulong) pos));

  DBUG_ASSERT(thd_get_ha_data(thd, binlog_hton) != NULL);
  /* Only true if binlog_trans_log_savepos() wasn't called before */
  DBUG_ASSERT(pos != ~(my_off_t) 0);

  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);
  trx_data->truncate(pos);
  DBUG_VOID_RETURN;
}


/*
  this function is mostly a placeholder.
  conceptually, binlog initialization (now mostly done in MYSQL_BIN_LOG::open)
  should be moved here.
*/

int binlog_init(void *p)
{
  binlog_hton= (handlerton *)p;
  binlog_hton->state=opt_bin_log ? SHOW_OPTION_YES : SHOW_OPTION_NO;
  binlog_hton->db_type=DB_TYPE_BINLOG;
  binlog_hton->savepoint_offset= sizeof(my_off_t);
  binlog_hton->close_connection= binlog_close_connection;
  binlog_hton->savepoint_set= binlog_savepoint_set;
  binlog_hton->savepoint_rollback= binlog_savepoint_rollback;
  binlog_hton->commit= binlog_commit;
  binlog_hton->rollback= binlog_rollback;
  binlog_hton->prepare= binlog_prepare;
  binlog_hton->flags= HTON_NOT_USER_SELECTABLE | HTON_HIDDEN;
  return 0;
}

static int binlog_close_connection(handlerton *hton, THD *thd)
{
  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);
  DBUG_ASSERT(trx_data->empty());
  thd_set_ha_data(thd, binlog_hton, NULL);
  trx_data->~binlog_trx_data();
  my_free((uchar*)trx_data, MYF(0));
  return 0;
}

/*
  End a transaction.

  SYNOPSIS
    binlog_end_trans()

    thd      The thread whose transaction should be ended
    trx_data Pointer to the transaction data to use
    end_ev   The end event to use, or NULL
    all      True if the entire transaction should be ended, false if
             only the statement transaction should be ended.

  DESCRIPTION

    End the currently open transaction. The transaction can be either
    a real transaction (if 'all' is true) or a statement transaction
    (if 'all' is false).

    If 'end_ev' is NULL, the transaction is a rollback of only
    transactional tables, so the transaction cache will be truncated
    to either just before the last opened statement transaction (if
    'all' is false), or reset completely (if 'all' is true).
 */
static int
binlog_end_trans(THD *thd, binlog_trx_data *trx_data,
                 Log_event *end_ev, bool all)
{
  DBUG_ENTER("binlog_end_trans");
  int error=0;
  IO_CACHE *trans_log= &trx_data->trans_log;
  DBUG_PRINT("enter", ("transaction: %s  end_ev: 0x%lx",
                       all ? "all" : "stmt", (long) end_ev));
  DBUG_PRINT("info", ("thd->options={ %s%s}",
                      FLAGSTR(thd->options, OPTION_NOT_AUTOCOMMIT),
                      FLAGSTR(thd->options, OPTION_BEGIN)));

  /*
    NULL denotes ROLLBACK with nothing to replicate: i.e., rollback of
    only transactional tables.  If the transaction contain changes to
    any non-transactiona tables, we need write the transaction and log
    a ROLLBACK last.
  */
  if (end_ev != NULL)
  {
    if (thd->binlog_flush_pending_rows_event(TRUE))
      DBUG_RETURN(1);
    /*
      Doing a commit or a rollback including non-transactional tables,
      i.e., ending a transaction where we might write the transaction
      cache to the binary log.

      We can always end the statement when ending a transaction since
      transactions are not allowed inside stored functions.  If they
      were, we would have to ensure that we're not ending a statement
      inside a stored function.
     */
    error= mysql_bin_log.write(thd, &trx_data->trans_log, end_ev,
                               trx_data->has_incident());
    trx_data->reset();

    statistic_increment(binlog_cache_use, &LOCK_status);
    if (trans_log->disk_writes != 0)
    {
      statistic_increment(binlog_cache_disk_use, &LOCK_status);
      trans_log->disk_writes= 0;
    }
  }
  else
  {
    /*
      If rolling back an entire transaction or a single statement not
      inside a transaction, we reset the transaction cache.

      If rolling back a statement in a transaction, we truncate the
      transaction cache to remove the statement.
     */
    thd->binlog_remove_pending_rows_event(TRUE);
    if (all || !(thd->options & (OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT)))
    {
      if (trx_data->has_incident())
        error= mysql_bin_log.write_incident(thd, TRUE, NULL);
      trx_data->reset();
    }
    else                                        // ...statement
      trx_data->truncate(trx_data->before_stmt_pos);
  }

  DBUG_ASSERT(thd->binlog_get_pending_rows_event() == NULL);
  DBUG_EXECUTE_IF("crash_binlog_end_trans_after", DBUG_SUICIDE(););
  DBUG_RETURN(error);
}

static int binlog_prepare(handlerton *hton, THD *thd, bool all)
{
  /*
    do nothing.
    just pretend we can do 2pc, so that MySQL won't
    switch to 1pc.
    real work will be done in MYSQL_BIN_LOG::log_xid()
  */
  return 0;
}

/**
  This function is called once after each statement.

  It has the responsibility to flush the transaction cache to the
  binlog file on commits.

  @param hton  The binlog handlerton.
  @param thd   The client thread that executes the transaction.
  @param all   This is @c true if this is a real transaction commit, and
               @false otherwise.

  @see handlerton::commit
*/
static int binlog_commit(handlerton *hton, THD *thd, bool all)
{
  int error= 0;
  DBUG_ENTER("binlog_commit");
  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);

  if (trx_data->empty())
  {
    // we're here because trans_log was flushed in MYSQL_BIN_LOG::log_xid()
    trx_data->reset();
    DBUG_RETURN(0);
  }

  /*
    We flush the cache if:

     - we are committing a transaction or;
     - no statement was committed before and just non-transactional
       tables were updated.

    Otherwise, we collect the changes.
  */
  DBUG_PRINT("debug",
             ("all: %d, empty: %s, all.modified_non_trans_table: %s, stmt.modified_non_trans_table: %s",
              all,
              YESNO(trx_data->empty()),
              YESNO(thd->transaction.all.modified_non_trans_table),
              YESNO(thd->transaction.stmt.modified_non_trans_table)));
  if (ending_trans(thd, all) ||
      (trans_has_no_stmt_committed(thd, all) &&
       !stmt_has_updated_trans_table(thd) && stmt_has_updated_non_trans_table(thd)))
  {
    Query_log_event qev(thd, STRING_WITH_LEN("COMMIT"), TRUE, TRUE, 0);
    error= binlog_end_trans(thd, trx_data, &qev, all);
  }

  trx_data->at_least_one_stmt_committed = my_b_tell(&trx_data->trans_log) > 0;

  if (!all)
    trx_data->before_stmt_pos = MY_OFF_T_UNDEF; // part of the stmt commit
  DBUG_RETURN(error);
}

/**
  This function is called when a transaction involving a transactional
  table is rolled back.

  It has the responsibility to flush the transaction cache to the
  binlog file. However, if the transaction does not involve
  non-transactional tables, nothing needs to be logged.

  @param hton  The binlog handlerton.
  @param thd   The client thread that executes the transaction.
  @param all   This is @c true if this is a real transaction rollback, and
               @false otherwise.

  @see handlerton::rollback
*/
static int binlog_rollback(handlerton *hton, THD *thd, bool all)
{
  DBUG_ENTER("binlog_rollback");
  int error=0;
  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);

  if (trx_data->empty()) {
    trx_data->reset();
    DBUG_RETURN(0);
  }

  DBUG_PRINT("debug", ("all: %s, all.modified_non_trans_table: %s, stmt.modified_non_trans_table: %s",
                       YESNO(all),
                       YESNO(thd->transaction.all.modified_non_trans_table),
                       YESNO(thd->transaction.stmt.modified_non_trans_table)));
  if (mysql_bin_log.check_write_error(thd))
  {
    /*
      "all == true" means that a "rollback statement" triggered the error and
      this function was called. However, this must not happen as a rollback
      is written directly to the binary log. And in auto-commit mode, a single
      statement that is rolled back has the flag all == false.
    */
    DBUG_ASSERT(!all);
    /*
      We reach this point if either only transactional tables were modified or
      the effect of a statement that did not get into the binlog needs to be
      rolled back. In the latter case, if a statement changed non-transactional
      tables or had the OPTION_KEEP_LOG associated, we write an incident event
      to the binlog in order to stop slaves and notify users that some changes
      on the master did not get into the binlog and slaves will be inconsistent.
      On the other hand, if a statement is transactional, we just safely roll it
      back.
    */
    if ((stmt_has_updated_non_trans_table(thd) ||
        (thd->options & OPTION_KEEP_LOG)) &&
        mysql_bin_log.check_write_error(thd))
      trx_data->set_incident();
    error= binlog_end_trans(thd, trx_data, 0, all);
  }
  else
  {
   /*
      We flush the cache with a rollback, wrapped in a beging/rollback if:
        . aborting a transaction that modified a non-transactional table or
          the OPTION_KEEP_LOG is activate.
        . aborting a statement that modified both transactional and
          non-transactional tables but which is not in the boundaries of any
          transaction or there was no early change;
    */
    if ((ending_trans(thd, all) &&
        (trans_has_updated_non_trans_table(thd) ||
         (thd->options & OPTION_KEEP_LOG))) ||
        (trans_has_no_stmt_committed(thd, all) &&
         stmt_has_updated_non_trans_table(thd) &&
         thd->current_stmt_binlog_row_based))
    {
      Query_log_event qev(thd, STRING_WITH_LEN("ROLLBACK"), TRUE, TRUE, 0);
      error= binlog_end_trans(thd, trx_data, &qev, all);
    }
    /*
      Otherwise, we simply truncate the cache as there is no change on
      non-transactional tables as follows.
    */
    else if (ending_trans(thd, all) ||
             (!(thd->options & OPTION_KEEP_LOG) && !stmt_has_updated_non_trans_table(thd)))
      error= binlog_end_trans(thd, trx_data, 0, all);
  }
  if (!all)
    trx_data->before_stmt_pos = MY_OFF_T_UNDEF; // part of the stmt rollback
  DBUG_RETURN(error);
}

/**
  Cleanup the cache.

  @param thd   The client thread that wants to clean up the cache.
*/
void MYSQL_BIN_LOG::reset_gathered_updates(THD *thd)
{
  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);

  trx_data->reset();
}

void MYSQL_BIN_LOG::set_write_error(THD *thd)
{
  DBUG_ENTER("MYSQL_BIN_LOG::set_write_error");

  write_error= 1;

  if (check_write_error(thd))
    DBUG_VOID_RETURN;

  if (my_errno == EFBIG)
    my_message(ER_TRANS_CACHE_FULL, ER(ER_TRANS_CACHE_FULL), MYF(MY_WME));
  else
    my_error(ER_ERROR_ON_WRITE, MYF(MY_WME), name, errno);

  DBUG_VOID_RETURN;
}

bool MYSQL_BIN_LOG::check_write_error(THD *thd)
{
  DBUG_ENTER("MYSQL_BIN_LOG::check_write_error");

  bool checked= FALSE;

  if (!thd->is_error())
    DBUG_RETURN(checked);

  switch (thd->main_da.sql_errno())
  {
    case ER_TRANS_CACHE_FULL:
    case ER_ERROR_ON_WRITE:
    case ER_BINLOG_LOGGING_IMPOSSIBLE:
      checked= TRUE;
    break;
  }

  DBUG_RETURN(checked);
}

/**
  @note
  How do we handle this (unlikely but legal) case:
  @verbatim
    [transaction] + [update to non-trans table] + [rollback to savepoint] ?
  @endverbatim
  The problem occurs when a savepoint is before the update to the
  non-transactional table. Then when there's a rollback to the savepoint, if we
  simply truncate the binlog cache, we lose the part of the binlog cache where
  the update is. If we want to not lose it, we need to write the SAVEPOINT
  command and the ROLLBACK TO SAVEPOINT command to the binlog cache. The latter
  is easy: it's just write at the end of the binlog cache, but the former
  should be *inserted* to the place where the user called SAVEPOINT. The
  solution is that when the user calls SAVEPOINT, we write it to the binlog
  cache (so no need to later insert it). As transactions are never intermixed
  in the binary log (i.e. they are serialized), we won't have conflicts with
  savepoint names when using mysqlbinlog or in the slave SQL thread.
  Then when ROLLBACK TO SAVEPOINT is called, if we updated some
  non-transactional table, we don't truncate the binlog cache but instead write
  ROLLBACK TO SAVEPOINT to it; otherwise we truncate the binlog cache (which
  will chop the SAVEPOINT command from the binlog cache, which is good as in
  that case there is no need to have it in the binlog).
*/

static int binlog_savepoint_set(handlerton *hton, THD *thd, void *sv)
{
  DBUG_ENTER("binlog_savepoint_set");

  binlog_trans_log_savepos(thd, (my_off_t*) sv);
  /* Write it to the binary log */

  String log_query;
  if (log_query.append(STRING_WITH_LEN("SAVEPOINT ")) ||
      log_query.append("`") ||
      log_query.append(thd->lex->ident.str, thd->lex->ident.length) ||
      log_query.append("`"))
    DBUG_RETURN(1);
  int errcode= query_error_code(thd, thd->killed == THD::NOT_KILLED);
  Query_log_event qinfo(thd, log_query.c_ptr_safe(), log_query.length(),
                        TRUE, TRUE, errcode);
  DBUG_RETURN(mysql_bin_log.write(&qinfo));
}

static int binlog_savepoint_rollback(handlerton *hton, THD *thd, void *sv)
{
  DBUG_ENTER("binlog_savepoint_rollback");

  /*
    Write ROLLBACK TO SAVEPOINT to the binlog cache if we have updated some
    non-transactional table. Otherwise, truncate the binlog cache starting
    from the SAVEPOINT command.
  */
  if (unlikely(trans_has_updated_non_trans_table(thd) || 
               (thd->options & OPTION_KEEP_LOG)))
  {
    String log_query;
    if (log_query.append(STRING_WITH_LEN("ROLLBACK TO ")) ||
        log_query.append("`") ||
        log_query.append(thd->lex->ident.str, thd->lex->ident.length) ||
        log_query.append("`"))
      DBUG_RETURN(1);
    int errcode= query_error_code(thd, thd->killed == THD::NOT_KILLED);
    Query_log_event qinfo(thd, log_query.c_ptr_safe(), log_query.length(),
                          TRUE, TRUE, errcode);
    DBUG_RETURN(mysql_bin_log.write(&qinfo));
  }
  binlog_trans_log_truncate(thd, *(my_off_t*)sv);
  DBUG_RETURN(0);
}


int check_binlog_magic(IO_CACHE* log, const char** errmsg)
{
  char magic[4];
  DBUG_ASSERT(my_b_tell(log) == 0);

  if (my_b_read(log, (uchar*) magic, sizeof(magic)))
  {
    *errmsg = "I/O error reading the header from the binary log";
    sql_print_error("%s, errno=%d, io cache code=%d", *errmsg, my_errno,
		    log->error);
    return 1;
  }
  if (memcmp(magic, BINLOG_MAGIC, sizeof(magic)))
  {
    *errmsg = "Binlog has bad magic number;  It's not a binary log file that can be used by this version of MySQL";
    return 1;
  }
  return 0;
}


File open_binlog(IO_CACHE *log, const char *log_file_name, const char **errmsg)
{
  File file;
  DBUG_ENTER("open_binlog");

  if ((file = my_open(log_file_name, O_RDONLY | O_BINARY | O_SHARE, 
                      MYF(MY_WME))) < 0)
  {
    sql_print_error("Failed to open log (file '%s', errno %d)",
                    log_file_name, my_errno);
    *errmsg = "Could not open log file";
    goto err;
  }
  if (init_io_cache(log, file, IO_SIZE*2, READ_CACHE, 0, 0,
                    MYF(MY_WME|MY_DONT_CHECK_FILESIZE)))
  {
    sql_print_error("Failed to create a cache on log (file '%s')",
                    log_file_name);
    *errmsg = "Could not open log file";
    goto err;
  }
  if (check_binlog_magic(log,errmsg))
    goto err;
  DBUG_RETURN(file);

err:
  if (file >= 0)
  {
    my_close(file,MYF(0));
    end_io_cache(log);
  }
  DBUG_RETURN(-1);
}

#ifdef __NT__
static int eventSource = 0;

static void setup_windows_event_source()
{
  HKEY    hRegKey= NULL;
  DWORD   dwError= 0;
  TCHAR   szPath[MAX_PATH];
  DWORD dwTypes;

  if (eventSource)               // Ensure that we are only called once
    return;
  eventSource= 1;

  // Create the event source registry key
  dwError= RegCreateKey(HKEY_LOCAL_MACHINE,
                          "SYSTEM\\CurrentControlSet\\Services\\EventLog\\Application\\MySQL", 
                          &hRegKey);

  /* Name of the PE module that contains the message resource */
  GetModuleFileName(NULL, szPath, MAX_PATH);

  /* Register EventMessageFile */
  dwError = RegSetValueEx(hRegKey, "EventMessageFile", 0, REG_EXPAND_SZ,
                          (PBYTE) szPath, (DWORD) (strlen(szPath) + 1));

  /* Register supported event types */
  dwTypes= (EVENTLOG_ERROR_TYPE | EVENTLOG_WARNING_TYPE |
            EVENTLOG_INFORMATION_TYPE);
  dwError= RegSetValueEx(hRegKey, "TypesSupported", 0, REG_DWORD,
                         (LPBYTE) &dwTypes, sizeof dwTypes);

  RegCloseKey(hRegKey);
}

#endif /* __NT__ */


/**
  Find a unique filename for 'filename.#'.

  Set '#' to a number as low as possible.

  @return
    nonzero if not possible to get unique filename
*/

static int find_uniq_filename(char *name)
{
  long                  number= 0;
  uint                  i;
  char                  buff[FN_REFLEN];
  struct st_my_dir     *dir_info;
  reg1 struct fileinfo *file_info;
  ulong                 max_found=0;
  size_t		buf_length, length;
  char			*start, *end;
  DBUG_ENTER("find_uniq_filename");

  length= dirname_part(buff, name, &buf_length);
  start=  name + length;
  end=    strend(start);

  *end='.';
  length= (size_t) (end-start+1);

  if ((DBUG_EVALUATE_IF("error_unique_log_filename", 1, 
      !(dir_info = my_dir(buff,MYF(MY_DONT_SORT))))))
  {						// This shouldn't happen
    strmov(end,".1");				// use name+1
    DBUG_RETURN(1);
  }
  file_info= dir_info->dir_entry;
  for (i=dir_info->number_off_files ; i-- ; file_info++)
  {
    if (memcmp(file_info->name, start, length) == 0 &&
	test_if_number(file_info->name+length, &number,0))
    {
      set_if_bigger(max_found,(ulong) number);
    }
  }
  my_dirend(dir_info);

  *end++='.';
  DBUG_RETURN((sprintf(end,"%06ld",max_found+1) < 0));
}


void MYSQL_LOG::init(enum_log_type log_type_arg,
                     enum cache_type io_cache_type_arg)
{
  DBUG_ENTER("MYSQL_LOG::init");
  log_type= log_type_arg;
  io_cache_type= io_cache_type_arg;
  DBUG_PRINT("info",("log_type: %d", log_type));
  DBUG_VOID_RETURN;
}


bool MYSQL_LOG::init_and_set_log_file_name(const char *log_name,
                                           const char *new_name,
                                           enum_log_type log_type_arg,
                                           enum cache_type io_cache_type_arg)
{
  init(log_type_arg, io_cache_type_arg);

  if (new_name && !strmov(log_file_name, new_name))
    return TRUE;
  else if (!new_name && generate_new_name(log_file_name, log_name))
    return TRUE;

  return FALSE;
}


/*
  Open a (new) log file.

  SYNOPSIS
    open()

    log_name            The name of the log to open
    log_type_arg        The type of the log. E.g. LOG_NORMAL
    new_name            The new name for the logfile. This is only needed
                        when the method is used to open the binlog file.
    io_cache_type_arg   The type of the IO_CACHE to use for this log file

  DESCRIPTION
    Open the logfile, init IO_CACHE and write startup messages
    (in case of general and slow query logs).

  RETURN VALUES
    0   ok
    1   error
*/

bool MYSQL_LOG::open(const char *log_name, enum_log_type log_type_arg,
                     const char *new_name, enum cache_type io_cache_type_arg)
{
  char buff[FN_REFLEN];
  File file= -1;
  int open_flags= O_CREAT | O_BINARY;
  DBUG_ENTER("MYSQL_LOG::open");
  DBUG_PRINT("enter", ("log_type: %d", (int) log_type_arg));

  write_error= 0;

  if (!(name= my_strdup(log_name, MYF(MY_WME))))
  {
    name= (char *)log_name; // for the error message
    goto err;
  }

  if (init_and_set_log_file_name(name, new_name,
                                 log_type_arg, io_cache_type_arg))
    goto err;

  if (io_cache_type == SEQ_READ_APPEND)
    open_flags |= O_RDWR | O_APPEND;
  else
    open_flags |= O_WRONLY | ((log_type == LOG_BIN ||
                               log_type == LOG_RELAY) ? 0 : O_APPEND);

  db[0]= 0;

  if ((file= my_open(log_file_name, open_flags,
                     MYF(MY_WME | ME_WAITTANG))) < 0 ||
      init_io_cache(&log_file, file, IO_SIZE, io_cache_type,
                    my_tell(file, MYF(MY_WME)), 0,
                    MYF(MY_WME | MY_NABP |
                        ((log_type == LOG_BIN || log_type == LOG_RELAY) ?
                         MY_WAIT_IF_FULL : 0))))
    goto err;

  if (log_type == LOG_NORMAL)
  {
    char *end;
    int len=my_snprintf(buff, sizeof(buff), "%s, Version: %s (%s). "
#ifdef EMBEDDED_LIBRARY
                        "embedded library\n",
                        my_progname, server_version, MYSQL_COMPILATION_COMMENT
#elif __NT__
                        "started with:\nTCP Port: %s, Named Pipe: %s\n",
                        my_progname, server_version, MYSQL_COMPILATION_COMMENT,
                        mysqld_ports_str, mysqld_unix_port
#else
                        "started with:\nTcp port: %s  Unix socket: %s\n",
                        my_progname, server_version, MYSQL_COMPILATION_COMMENT,
                        mysqld_ports_str, mysqld_unix_port
#endif
                       );
    end= strnmov(buff + len, "Time                 Id Command    Argument\n",
                 sizeof(buff) - len);
    if (my_b_write(&log_file, (uchar*) buff, (uint) (end-buff)) ||
	flush_io_cache(&log_file))
      goto err;
  }

  log_state= LOG_OPENED;
  DBUG_RETURN(0);

err:
  sql_print_error("Could not use %s for logging (error %d). \
Turning logging off for the whole duration of the MySQL server process. \
To turn it on again: fix the cause, \
shutdown the MySQL server and restart it.", name, errno);
  if (file >= 0)
    my_close(file, MYF(0));
  end_io_cache(&log_file);
  safeFree(name);
  log_state= LOG_CLOSED;
  DBUG_RETURN(1);
}

MYSQL_LOG::MYSQL_LOG()
  : name(0), write_error(FALSE), inited(FALSE), log_type(LOG_UNKNOWN),
    log_state(LOG_CLOSED)
{
  /*
    We don't want to initialize LOCK_Log here as such initialization depends on
    safe_mutex (when using safe_mutex) which depends on MY_INIT(), which is
    called only in main(). Doing initialization here would make it happen
    before main().
  */
  bzero((char*) &log_file, sizeof(log_file));
}

void MYSQL_LOG::init_pthread_objects()
{
  DBUG_ASSERT(inited == 0);
  inited= 1;
  (void) pthread_mutex_init(&LOCK_log, MY_MUTEX_INIT_SLOW);
}

/*
  Close the log file

  SYNOPSIS
    close()
    exiting     Bitmask. For the slow and general logs the only used bit is
                LOG_CLOSE_TO_BE_OPENED. This is used if we intend to call
                open at once after close.

  NOTES
    One can do an open on the object at once after doing a close.
    The internal structures are not freed until cleanup() is called
*/

void MYSQL_LOG::close(uint exiting)
{					// One can't set log_type here!
  DBUG_ENTER("MYSQL_LOG::close");
  DBUG_PRINT("enter",("exiting: %d", (int) exiting));
  if (log_state == LOG_OPENED)
  {
    end_io_cache(&log_file);

    if (my_sync(log_file.file, MYF(MY_WME)) && ! write_error)
    {
      write_error= 1;
      sql_print_error(ER(ER_ERROR_ON_WRITE), name, errno);
    }

    if (my_close(log_file.file, MYF(MY_WME)) && ! write_error)
    {
      write_error= 1;
      sql_print_error(ER(ER_ERROR_ON_WRITE), name, errno);
    }
  }

  log_state= (exiting & LOG_CLOSE_TO_BE_OPENED) ? LOG_TO_BE_OPENED : LOG_CLOSED;
  safeFree(name);
  DBUG_VOID_RETURN;
}

/** This is called only once. */

void MYSQL_LOG::cleanup()
{
  DBUG_ENTER("cleanup");
  if (inited)
  {
    inited= 0;
    (void) pthread_mutex_destroy(&LOCK_log);
    close(0);
  }
  DBUG_VOID_RETURN;
}


int MYSQL_LOG::generate_new_name(char *new_name, const char *log_name)
{
  fn_format(new_name, log_name, mysql_data_home, "", 4);
  if (log_type == LOG_BIN || log_type == LOG_RELAY || log_type == LOG_SQL)
  {
    if (!fn_ext(log_name)[0])
    {
      if (find_uniq_filename(new_name))
      {
        my_printf_error(ER_NO_UNIQUE_LOGFILE, ER(ER_NO_UNIQUE_LOGFILE),
                        MYF(ME_FATALERROR), log_name);
	sql_print_error(ER(ER_NO_UNIQUE_LOGFILE), log_name);
	return 1;
      }
    }
  }
  return 0;
}


/*
  Reopen the log file

  SYNOPSIS
    reopen_file()

  DESCRIPTION
    Reopen the log file. The method is used during FLUSH LOGS
    and locks LOCK_log mutex
*/


void MYSQL_QUERY_LOG::reopen_file()
{
  char *save_name;

  DBUG_ENTER("MYSQL_LOG::reopen_file");
  if (!is_open())
  {
    DBUG_PRINT("info",("log is closed"));
    DBUG_VOID_RETURN;
  }

  pthread_mutex_lock(&LOCK_log);

  save_name= name;
  name= 0;				// Don't free name
  close(LOG_CLOSE_TO_BE_OPENED);

  /*
     Note that at this point, log_state != LOG_CLOSED (important for is_open()).
  */

  open(save_name, log_type, 0, io_cache_type);
  my_free(save_name, MYF(0));

  pthread_mutex_unlock(&LOCK_log);

  DBUG_VOID_RETURN;
}


/*
  Write a command to traditional general log file

  SYNOPSIS
    write()

    event_time        command start timestamp
    user_host         the pointer to the string with user@host info
    user_host_len     length of the user_host string. this is computed once
                      and passed to all general log  event handlers
    thread_id         Id of the thread, issued a query
    command_type      the type of the command being logged
    command_type_len  the length of the string above
    sql_text          the very text of the query being executed
    sql_text_len      the length of sql_text string

  DESCRIPTION

   Log given command to to normal (not rotable) log file

  RETURN
    FASE - OK
    TRUE - error occured
*/

bool MYSQL_QUERY_LOG::write(time_t event_time, const char *user_host,
                            uint user_host_len, int thread_id,
                            const char *command_type, uint command_type_len,
                            const char *sql_text, uint sql_text_len)
{
  char buff[32];
  uint length= 0;
  char local_time_buff[MAX_TIME_SIZE];
  struct tm start;
  uint time_buff_len= 0;

  (void) pthread_mutex_lock(&LOCK_log);

  /* Test if someone closed between the is_open test and lock */
  if (is_open())
  {
    /* for testing output of timestamp and thread id */
    DBUG_EXECUTE_IF("reset_log_last_time", last_time= 0;);

    /* Note that my_b_write() assumes it knows the length for this */
      if (event_time != last_time)
      {
        last_time= event_time;

        localtime_r(&event_time, &start);

        time_buff_len= my_snprintf(local_time_buff, MAX_TIME_SIZE,
                                   "%02d%02d%02d %2d:%02d:%02d\t",
                                   start.tm_year % 100, start.tm_mon + 1,
                                   start.tm_mday, start.tm_hour,
                                   start.tm_min, start.tm_sec);

        if (my_b_write(&log_file, (uchar*) local_time_buff, time_buff_len))
          goto err;
      }
      else
        if (my_b_write(&log_file, (uchar*) "\t\t" ,2) < 0)
          goto err;

      /* command_type, thread_id */
      length= my_snprintf(buff, 32, "%5ld ", (long) thread_id);

    if (my_b_write(&log_file, (uchar*) buff, length))
      goto err;

    if (my_b_write(&log_file, (uchar*) command_type, command_type_len))
      goto err;

    if (my_b_write(&log_file, (uchar*) "\t", 1))
      goto err;

    /* sql_text */
    if (my_b_write(&log_file, (uchar*) sql_text, sql_text_len))
      goto err;

    if (my_b_write(&log_file, (uchar*) "\n", 1) ||
        flush_io_cache(&log_file))
      goto err;
  }

  (void) pthread_mutex_unlock(&LOCK_log);
  return FALSE;
err:

  if (!write_error)
  {
    write_error= 1;
    sql_print_error(ER(ER_ERROR_ON_WRITE), name, errno);
  }
  (void) pthread_mutex_unlock(&LOCK_log);
  return TRUE;
}


/*
  Log a query to the traditional slow log file

  SYNOPSIS
    write()

    thd               THD of the query
    current_time      current timestamp
    query_start_arg   command start timestamp
    user_host         the pointer to the string with user@host info
    user_host_len     length of the user_host string. this is computed once
                      and passed to all general log event handlers
    query_utime       Amount of time the query took to execute (in microseconds)
    lock_utime        Amount of time the query was locked (in microseconds)
    is_command        The flag, which determines, whether the sql_text is a
                      query or an administrator command.
    sql_text          the very text of the query or administrator command
                      processed
    sql_text_len      the length of sql_text string

  DESCRIPTION

   Log a query to the slow log file.

  RETURN
    FALSE - OK
    TRUE - error occured
*/

bool MYSQL_QUERY_LOG::write(THD *thd, time_t current_time,
                            time_t query_start_arg, const char *user_host,
                            uint user_host_len, ulonglong query_utime,
                            ulonglong lock_utime, bool is_command,
                            const char *sql_text, uint sql_text_len)
{
  bool error= 0;
  DBUG_ENTER("MYSQL_QUERY_LOG::write");

  (void) pthread_mutex_lock(&LOCK_log);

  if (!is_open())
  {
    (void) pthread_mutex_unlock(&LOCK_log);
    DBUG_RETURN(0);
  }

  if (is_open())
  {						// Safety agains reopen
    int tmp_errno= 0;
    char buff[80], *end;
    char query_time_buff[22+7], lock_time_buff[22+7];
    uint buff_len;
    end= buff;

    if (!(specialflag & SPECIAL_SHORT_LOG_FORMAT))
    {
      if (current_time != last_time)
      {
        last_time= current_time;
        struct tm start;
        localtime_r(&current_time, &start);

        buff_len= my_snprintf(buff, sizeof buff,
                              "# Time: %02d%02d%02d %2d:%02d:%02d\n",
                              start.tm_year % 100, start.tm_mon + 1,
                              start.tm_mday, start.tm_hour,
                              start.tm_min, start.tm_sec);

        /* Note that my_b_write() assumes it knows the length for this */
        if (my_b_write(&log_file, (uchar*) buff, buff_len))
          tmp_errno= errno;
      }
      const uchar uh[]= "# User@Host: ";
      if (my_b_write(&log_file, uh, sizeof(uh) - 1))
        tmp_errno= errno;
      if (my_b_write(&log_file, (uchar*) user_host, user_host_len))
        tmp_errno= errno;
      if (my_b_write(&log_file, (uchar*) "\n", 1))
        tmp_errno= errno;
    }
    /* For slow query log */
    sprintf(query_time_buff, "%.6f", ulonglong2double(query_utime)/1000000.0);
    sprintf(lock_time_buff,  "%.6f", ulonglong2double(lock_utime)/1000000.0);
    if (my_b_printf(&log_file,
                    "# Query_time: %s  Lock_time: %s"
                    " Rows_sent: %lu  Rows_examined: %lu\n",
                    query_time_buff, lock_time_buff,
                    (ulong) thd->sent_row_count,
                    (ulong) thd->examined_row_count) == (uint) -1)
      tmp_errno= errno;
    if (thd->db && strcmp(thd->db, db))
    {						// Database changed
      if (my_b_printf(&log_file,"use %s;\n",thd->db) == (uint) -1)
        tmp_errno= errno;
      strmov(db,thd->db);
    }
    if (thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt)
    {
      end=strmov(end, ",last_insert_id=");
      end=longlong10_to_str((longlong)
                            thd->first_successful_insert_id_in_prev_stmt_for_binlog,
                            end, -10);
    }
    // Save value if we do an insert.
    if (thd->auto_inc_intervals_in_cur_stmt_for_binlog.nb_elements() > 0)
    {
      if (!(specialflag & SPECIAL_SHORT_LOG_FORMAT))
      {
        end=strmov(end,",insert_id=");
        end=longlong10_to_str((longlong)
                              thd->auto_inc_intervals_in_cur_stmt_for_binlog.minimum(),
                              end, -10);
      }
    }

    /*
      This info used to show up randomly, depending on whether the query
      checked the query start time or not. now we always write current
      timestamp to the slow log
    */
    end= strmov(end, ",timestamp=");
    end= int10_to_str((long) current_time, end, 10);

    if (end != buff)
    {
      *end++=';';
      *end='\n';
      if (my_b_write(&log_file, (uchar*) "SET ", 4) ||
          my_b_write(&log_file, (uchar*) buff + 1, (uint) (end-buff)))
        tmp_errno= errno;
    }
    if (is_command)
    {
      end= strxmov(buff, "# administrator command: ", NullS);
      buff_len= (ulong) (end - buff);
      my_b_write(&log_file, (uchar*) buff, buff_len);
    }
    if (my_b_write(&log_file, (uchar*) sql_text, sql_text_len) ||
        my_b_write(&log_file, (uchar*) ";\n",2) ||
        flush_io_cache(&log_file))
      tmp_errno= errno;
    if (tmp_errno)
    {
      error= 1;
      if (! write_error)
      {
        write_error= 1;
        sql_print_error(ER(ER_ERROR_ON_WRITE), name, error);
      }
    }
  }
  (void) pthread_mutex_unlock(&LOCK_log);
  DBUG_RETURN(error);
}

int MYSQL_SQL_LOG::new_file()
{
  char *save_name;
  int result= 0;

  if (is_open())
  {
    pthread_mutex_lock(&LOCK_log);

    save_name= name;
    name= 0;                                  /* Don't free name. */

    close(LOG_CLOSE_TO_BE_OPENED);

    /*
      Note that at this point, log_state != LOG_CLOSED
      (important for is_open()).
    */

    if (open(save_name, log_type, NULL, io_cache_type))
      result= 1;
    my_free(save_name, MYF(0));

    pthread_mutex_unlock(&LOCK_log);
  }

  return result;
}

/**
  Checks if a field in a record is SQL NULL.

  This function is copied from ha_federated.cc. This method uses the
  record format information in table to track the null bit in record.

  @param  table   MySQL table object
  @param  field   MySQL field object
  @param  record  contains record

  @return Operation status
    @retval 1     if NULL
    @retval 0     otherwise
*/

static inline uint field_in_record_is_null(TABLE *table,
                                           Field *field,
                                           const uchar *record)
{
  int null_offset;
  DBUG_ENTER("field_in_record_is_null");

  if (!field->null_ptr)
    DBUG_RETURN(0);

  null_offset= (uint) (field->null_ptr - table->record[0]);

  if (record[null_offset] & field->null_bit)
    DBUG_RETURN(1);

  DBUG_RETURN(0);
}

enum_sqllog_stmt_type sql_command2sqllog_type(enum_sql_command sql_command)
{
  switch (sql_command)
  {
  case SQLCOM_TRUNCATE:       return SQLLOG_STMT_TRUNCATE_TABLE;
  case SQLCOM_CREATE_TABLE:   return SQLLOG_STMT_CREATE_TABLE;
  case SQLCOM_ALTER_TABLE:    return SQLLOG_STMT_ALTER_TABLE;
  case SQLCOM_RENAME_TABLE:   return SQLLOG_STMT_RENAME_TABLE;
  case SQLCOM_DROP_TABLE:     return SQLLOG_STMT_DROP_TABLE;
  case SQLCOM_CREATE_INDEX:   return SQLLOG_STMT_CREATE_INDEX;
  case SQLCOM_DROP_INDEX:     return SQLLOG_STMT_DROP_INDEX;
  case SQLCOM_CREATE_DB:      return SQLLOG_STMT_CREATE_DB;
  case SQLCOM_ALTER_DB:       return SQLLOG_STMT_ALTER_DB;
  case SQLCOM_DROP_DB:        return SQLLOG_STMT_DROP_DB;
  default:                    return SQLLOG_STMT_IGNORE;
  }
}

/**
  Returns if sql log should be logged.

  @return Operation status
    @retval true   sql log should be logged
    @retval false  sql log should not be logged
*/

bool MYSQL_SQL_LOG::should_log(THD *thd)
{
  if (opt_sql_log_as_master)
    /* We are logging as a master, we log only non-replication workload. */
    return opt_sql_log && !thd->slave_thread;
  else
    /* We are logging as a slave, we log only replication workload on slave. */
    return opt_sql_log && thd->slave_thread;
}

/**
  Log a string to the sql log file

  @param  thd        THD of the query
  @param  text       string to write to the sql_log.  It is not required that
                     it is the entire line for the sql_log.
  @param  length     number of bytes of 'text' to write.
  @param  line_done  if true, the line for the sql_log is done, and '\n'
                     will be appended.  Otherwise, no '\n' will be written.

  @return Operation status
    @retval false    ok
    @retval true     error occurred
*/

bool MYSQL_SQL_LOG::sql_log_write(THD *thd, const uchar *text, int length,
                                  bool line_done)
{
  bool error= false;
  int tmp_errno= 0;
  int err= 0;

  if (!is_open())
    return 0;

  err= my_b_write(&log_file, text, length);
  /* Only write the '\n' at the end of the line. */
  if (err == 0 && line_done)
    err= my_b_write(&log_file, "\n", 1);

  if (err != 0 || (line_done && flush_io_cache(&log_file)))
    tmp_errno= errno;
  if (tmp_errno)
  {
    /* Send a logging failure message to the general error log.  */
    sql_print_error("Sql log TXN failure: %s", text);
    error= true;
    /*
      write_error is set zero upon file open and never reset to zero.  If we
      have errored then this guard ensures we only write the message once.
    */
    if (!write_error)
    {
      write_error= 1;
      sql_print_error(ER(ER_ERROR_ON_WRITE), name, tmp_errno);
    }
  }

  /*
    Rotate the log if it exceeds max_binlog_size.  Only check if the line is
    done.
  */
  if (line_done && my_b_tell(&log_file) >= max_binlog_size)
    new_file();

  return error;
}

bool MYSQL_SQL_LOG::log_ddl(THD* thd)
{
  bool error= 0;
  if (opt_sql_log_ddl && should_log(thd))
  {
    if (thd->lex)
    {
      LEX *lex= thd->lex;
      enum_sqllog_stmt_type sqllog_type=
        sql_command2sqllog_type(lex->sql_command);

      /*
        NOTE: The CREATE TABLE statement is a special case because all the
        information about the newly created table is lost when the bin log
        was written. We handle this special case in sql_parse.cc.
      */
      if (sqllog_type != SQLLOG_STMT_IGNORE &&
          sqllog_type != SQLLOG_STMT_CREATE_TABLE)
      {
        DBUG_EXECUTE_IF("sleep_in_sqllog_log_ddl", sleep(3););

        /* The first SELECT_LEX contains the target table of DDL commands. */
        SELECT_LEX *select_lex= &lex->select_lex;
        /* First table of first SELECT_LEX */
        TABLE_LIST *first_table= (TABLE_LIST *) select_lex->table_list.first;
        /*
          !IMPORTANT: The statement types the current sql log is interested
          in have the first table as its main table. Please verify this when
          adding new sqllog DDL statement types.
        */
        if (first_table != 0)
        {
          if (!opt_sql_log_database ||
              strstr(first_table->db, opt_sql_log_database))
            error= thd->sqllog_log(sqllog_type, first_table->db,
                                   first_table->table_name, thd->query(),
                                   thd->query_length());
          else
            /* Ignore the non-target database, return OK. */
            return 0;
        }
        else
        {
          /*
            No table is in the statement. The statement is create db, alter
            db or drop db statement. We will log all of these statements with
            database as "N/A", as the modified database probably is different
            from the default database stored in "thd->db".
          */
          if (thd->db)
            error= thd->sqllog_log(sqllog_type, "N/A", "N/A", thd->query(),
                                   thd->query_length());
        }
        /*
          Since mysql does not support DDL transaction right now, we can
          safely log the DDL statement no matter it is in a transaction or
          not.
        */
        if (!error)
          error= thd->sqllog_commit();
      }
    }
  }
  return (unlikely(error)) ? HA_ERR_SQL_LOG_TXN : 0;
}

static bool sqllog_convert_field(Field *field, String &scratch,
                                 String &result, const uchar *offset= NULL)
{
  bool error= false;
  /*
    This is scratch space for holding field data that will be appended
    to the result string.  Ensure it is cleared here.  We prefer that the
    calling function define the scratch-space, to reduce frequent
    memory allocations as this function can be called often.
  */
  scratch.length(0);

  /* Save the old read_set to restore it later. */
  my_bitmap_map *old_map= tmp_use_all_columns(field->table,
                                              field->table->read_set);

  /*
    Where there is an supplied offset (for UPDATE statements), we need
    to adjust the internal field 'ptr' to point to the old data, in order
    to find out the old column values that we are replacing.
  */
  uchar *old_ptr;
  if (offset)
  {
    old_ptr= field->ptr;
    field->ptr= const_cast<uchar *>(offset);
  }

  if (field->type() == MYSQL_TYPE_BIT)
  {
    /* Handle the BIT type. */

    /*
      Converts a bit field, internally represented as a longlong, into
      a string of binary digits in the format: b'<digits>'.
    */
    ulonglong bits= (ulonglong) field->val_int();
    if (bits)
    {
      char temp[(sizeof(longlong) * 8) + 4];
      int i= sizeof(temp) - 1;

      temp[i--]= '\0';
      temp[i--]= '\'';
      while (bits)
      {
        temp[i--]= (bits & 1) ? '1' : '0';
        bits>>= 1;
      }
      temp[i--]= '\'';
      temp[i]= 'b';

      error|= result.append(temp + i);
    }
    else
      error|= result.append("b'0'");
  }
  else if (field->result_type() == REAL_RESULT)
  {
    /* Handle floats and doubles. */
    double value= field->val_real();
    int num_chars=
      Item_func_ieee754_to_string::convert_real_to_string(value, &scratch);
    if (num_chars < 1 || num_chars >= 30)
    {
      sql_print_error("sqllog_convert_field: convert_real_to_string failed.");
      error= true;
    }
    else
      scratch.print(&result);
  }
  else
  {
    /* Handle all other data types. */
    field->val_str(&scratch);

    /*
      all string data columns including blobs are in base64 form.
      e.g., name = b64'Z29vZ2xlIGN1cA=='
    */
    if (field->str_needs_quotes())
    {
      error|= result.append("b64'");
      error|= scratch.print64(&result);
      error|= result.append('\'');
    }
    else
      scratch.print(&result);
  }

  /* Change the field back to pointing at the new data. */
  if (offset)
    field->ptr= old_ptr;

  /* Restore the previous state of the read_set. */
  tmp_restore_column_map(field->table->read_set, old_map);

  return error;
}

/**
  Returns the shortened data type string.

  The data types are listed in the enum enum_field_types defined in
  include/mysql_com.h. The "MYSQL_TYPE_" prefix will be stripped from the type
  string. For example, the fuction will ouptput "DECIMAL" for mysql internal
  type MYSQL_TYPE_DECIMAL.

  This function looks very much like the function field_type_name() defined in
  sql/sql_class.cc. That function is a local function and not currently used.
  We can make it accessible by changing it to a global function or a virtual
  member function for the Field class. However the purpose of the function is
  slightly different form this function (we need shortened type name here) and
  the code change involved will make the future merge difficult. So the
  decision here is to add a new local function as this one.
*/

static char const *short_field_type_name(enum_field_types type)
{
  switch (type) {
  case MYSQL_TYPE_DECIMAL:
    return "DECIMAL";
  case MYSQL_TYPE_TINY:
    return "TINY";
  case MYSQL_TYPE_SHORT:
    return "SHORT";
  case MYSQL_TYPE_LONG:
    return "LONG";
  case MYSQL_TYPE_FLOAT:
    return "FLOAT";
  case MYSQL_TYPE_DOUBLE:
    return "DOUBLE";
  case MYSQL_TYPE_NULL:
    return "NULL";
  case MYSQL_TYPE_TIMESTAMP:
    return "TIMESTAMP";
  case MYSQL_TYPE_LONGLONG:
    return "LONGLONG";
  case MYSQL_TYPE_INT24:
    return "INT24";
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_NEWDATE:
    return "DATE";
  case MYSQL_TYPE_TIME:
    return "TIME";
  case MYSQL_TYPE_DATETIME:
    return "DATETIME";
  case MYSQL_TYPE_YEAR:
    return "YEAR";
  case MYSQL_TYPE_BIT:
    return "BIT";
  case MYSQL_TYPE_NEWDECIMAL:
    return "NEWDECIMAL";
  case MYSQL_TYPE_ENUM:
    return "ENUM";
  case MYSQL_TYPE_SET:
    return "SET";
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_BLOB:
    return "BLOB";
  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_STRING:
    return "STRING";
  case MYSQL_TYPE_GEOMETRY:
    return "GEOMETRY";
  }
  return "UNKNOWN";
}

/**
  Appends the data type string to the result string.

  An empty space will be added to both the beginning and the end of the type
  string, e.g., the string " DECIMAL " will be added to the result string for a
  MySQL_TYPE_DECIMAL field.
*/

static bool sqllog_add_field_type(Field *field, String &result)
{
  bool error= false;
  error|= result.append(" ");
  error|= result.append(short_field_type_name(field->real_type()));
  error|= result.append(" ");
  return error;
}

/**
  Create an SQL representation of an INSERT statement.

  @param  thd     current thread
  @param  table   table for insert
  @param  buf     insert fields
  @param  insert  string containing resultant statement

  @return Operation status
    @retval false    ok
    @retval true     error occurred
*/

bool sqllog_write_row(THD *thd, TABLE *table, const uchar *buf, String *insert)
{
  DBUG_ENTER("sqllog_write_row");
  DBUG_ASSERT(table->s->tmp_table == NO_TMP_TABLE);

  bool error= false;

  String scratch(STRING_BUFFER_USUAL_SIZE);

  insert->length(0);

  /*
    Loop through the field pointer array, add any fields to both the values
    list and the fields list that match the current query id.
  */
  bool commas= false;
  Field **ptr, *field;
  for (ptr= table->field; (field= *ptr) && !error; ptr++)
  {
    commas= true;

    /* Append the field name. */
    error|= insert->append(field->field_name);
    /* Append the field type. */
    error|= sqllog_add_field_type(field, *insert);

    if (field->is_null())
      insert->append("NULL");
    else
      error|= sqllog_convert_field(field, scratch, *insert);

    error|= insert->append(" AND ");
  }

  if (commas)
  {
    /* Chop off trailing ' AND '. */
    for (uint i= 0; i < sizeof("AND "); i++)
      insert->chop();
  }

  (*insert)[insert->length()]= '\0';

  DBUG_RETURN(error);
}

/**
  Create an SQL representation of an UPDATE statement.

  @param  thd       current thread
  @param  table     table for update
  @param  old_data  old field data to be replaced
  @param  new_data  new field data to be inserted
  @param  update    string containing resultant statement

  @return Operation status
    @retval false    ok
    @retval true     error occurred
*/

bool sqllog_update_row(THD *thd, TABLE *table, const uchar *old_data,
                       const uchar *new_data, String *update)
{
  DBUG_ENTER("sqllog_update_row");
  DBUG_ASSERT(table->s->tmp_table == NO_TMP_TABLE);

  bool error= false;

  String where(STRING_BUFFER_USUAL_SIZE);
  String scratch(STRING_BUFFER_USUAL_SIZE);

  update->length(0);
  where.length(0);

  /*
    In this loop, we want to match column names to values being inserted
    (while building INSERT statement).

    Iterate through table->field (new data) and share->old_field (old_data)
    using the same index to create an SQL UPDATE statement. New data is
    used to create SET field=value and old data is used to create WHERE
    field=oldvalue.
  */

  Field **ptr, *field;
  for (ptr= table->field; (field= *ptr) && !error; ptr++)
  {
    /*
      Within the bitmap, the write bits will be set for any field (column)
      that is being updated with new data.

      With RBR all bits are set so also check that the new field value is
      different from the old field value.
    */
    if (bitmap_is_set(table->write_set, field->field_index) &&
        field->cmp_binary(new_data + field->offset(table->record[0]),
                          old_data + field->offset(table->record[0])))
    {
      /* Append the field name. */
      error|= update->append(field->field_name);
      /* Append the field type. */
      error|= sqllog_add_field_type(field, *update);

      if (field->is_null())
        error|= update->append("NULL");
      else
        /* otherwise = */
        error|= sqllog_convert_field(field, scratch, *update);
      error|= update->append(" AND ");
    }

    /* Record every field. */
    error|= where.append(field->field_name);
    /* Append the field type. */
    error|= sqllog_add_field_type(field, where);

    if (field_in_record_is_null(table, field, old_data))
      error|= where.append("NULL");
    else
      error|= sqllog_convert_field(field, scratch, where,
                                   old_data + field->offset(table->record[0]));
    error|= where.append(" AND ");
  }

  if (update->length())
    /* Chop off trailing 'AND ' */
    for (uint i= 0; i < sizeof("AND "); i++)
      update->chop();

  if (where.length())
  {
    /* Chop off trailing 'AND '. */
    for (uint i= 0; i < sizeof("AND "); i++)
      where.chop();

    error|= update->append(" WHERE ");
    error|= update->append(where);
  }

  (*update)[update->length()]= '\0';

  DBUG_PRINT("info", ("Update sql: %s", update->c_ptr_quick()));
  DBUG_RETURN(error);
}

/**
  Create an SQL representation of a DELETE statement.

  @param  thd    current thread
  @param  table  table for insert
  @param  buf    fields data
  @param  obj    string containing resultant statement

  @return Operation status
    @retval false    ok
    @retval true     error occurred
*/

bool sqllog_delete_row(THD *thd, TABLE *table, const uchar *buf, String *obj)
{
  DBUG_ENTER("sqllog_delete_row");
  DBUG_ASSERT(table->s->tmp_table == NO_TMP_TABLE);

  bool error= false;
  String scratch(STRING_BUFFER_USUAL_SIZE);

  obj->length(0);
  error|= obj->append("WHERE ");

  Field **ptr, *field;
  bool any_fields= false;
  for (ptr= table->field; (field= *ptr) && !error; ptr++)
  {
    any_fields= true;
    error|= obj->append(field->field_name);
    /* Append the field type. */
    error|= sqllog_add_field_type(field, *obj);

    if (field_in_record_is_null(table, field, buf))
      error|= obj->append("NULL");
    else
      error|= sqllog_convert_field(field, scratch, *obj,
                                   buf + field->offset(table->record[0]));

    /* If another field exists, add 'AND'. */
    error|= obj->append(" AND ");
  }

  /* Chop off trailing ' AND '. */
  if (any_fields)
    for (uint i= 0; i < sizeof("AND "); i++)
      obj->chop();

  (*obj)[obj->length()]= '\0';

  /* Remove the WHERE string, if there are no user specified fields. */
  if (!any_fields)
    obj->length(0);

  DBUG_PRINT("info", ("Delete sql: %s", obj->c_ptr_quick()));
  DBUG_RETURN(error);
}

/**
  @todo
  The following should be using fn_format();  We just need to
  first change fn_format() to cut the file name if it's too long.
*/
const char *MYSQL_LOG::generate_name(const char *log_name,
                                      const char *suffix,
                                      bool strip_ext, char *buff)
{
  if (!log_name || !log_name[0])
  {
    strmake(buff, pidfile_name, FN_REFLEN - strlen(suffix) - 1);
    return (const char *)
      fn_format(buff, buff, "", suffix, MYF(MY_REPLACE_EXT|MY_REPLACE_DIR));
  }
  // get rid of extension if the log is binary to avoid problems
  if (strip_ext)
  {
    char *p= fn_ext(log_name);
    uint length= (uint) (p - log_name);
    strmake(buff, log_name, min(length, FN_REFLEN-1));
    return (const char*)buff;
  }
  return log_name;
}



MYSQL_BIN_LOG::MYSQL_BIN_LOG()
  :dump_thd_count(0),
  resetting_logs(FALSE),
  bytes_written(0),
  prepared_xids(0),
  file_id(1),
  open_count(1),
  group_id(0),
  last_event_server_id(0),
  have_master(false),
  do_rpl_hierarchical_slave_recovery(false),
  group_id_only_crash_recovery(false),
  is_relay_log(0),
  description_event_for_exec(0),
  description_event_for_queue(0)
{
  /*
    We don't want to initialize locks here as such initialization depends on
    safe_mutex (when using safe_mutex) which depends on MY_INIT(), which is
    called only in main(). Doing initialization here would make it happen
    before main().
  */
  index_file_name[0] = 0;
  bzero((char*) &index_file, sizeof(index_file));
  bzero((char*) &purge_index_file, sizeof(purge_index_file));
}

/* this is called only once */

void MYSQL_BIN_LOG::cleanup()
{
  DBUG_ENTER("cleanup");
  if (inited)
  {
    inited= 0;
    close(LOG_CLOSE_INDEX|LOG_CLOSE_STOP_EVENT);
    delete description_event_for_queue;
    delete description_event_for_exec;
    (void) pthread_mutex_destroy(&LOCK_log);
    (void) pthread_mutex_destroy(&LOCK_index);
    (void) pthread_cond_destroy(&update_cond);
    (void) pthread_mutex_destroy(&LOCK_reset_logs);
    (void) pthread_cond_destroy(&COND_no_dump_thd);
    (void) pthread_cond_destroy(&COND_no_reset_thd);
  }
  DBUG_VOID_RETURN;
}


/* Init binlog-specific vars */
void MYSQL_BIN_LOG::init(bool no_auto_events_arg, ulong max_size_arg)
{
  DBUG_ENTER("MYSQL_BIN_LOG::init");
  no_auto_events= no_auto_events_arg;
  max_size= max_size_arg;
  DBUG_PRINT("info",("max_size: %lu", max_size));
  DBUG_VOID_RETURN;
}


void MYSQL_BIN_LOG::init_pthread_objects()
{
  DBUG_ASSERT(inited == 0);
  inited= 1;
  (void) pthread_mutex_init(&LOCK_log, MY_MUTEX_INIT_SLOW);
  (void) pthread_mutex_init(&LOCK_index, MY_MUTEX_INIT_SLOW);
  (void) pthread_cond_init(&update_cond, 0);
  (void) pthread_mutex_init(&LOCK_reset_logs, MY_MUTEX_INIT_FAST);
  (void) pthread_cond_init(&COND_no_dump_thd, 0);
  (void) pthread_cond_init(&COND_no_reset_thd, 0);
}


int MYSQL_BIN_LOG::open_index_file(const char *index_file_name_arg,
                                   const char *log_name, bool need_mutex)
{
  File index_file_nr= -1;
  DBUG_ASSERT(!my_b_inited(&index_file));

  /*
    First open of this class instance
    Create an index file that will hold all file names uses for logging.
    Add new entries to the end of it.
  */
  myf opt= MY_UNPACK_FILENAME;
  if (!index_file_name_arg)
  {
    index_file_name_arg= log_name;    // Use same basename for index file
    opt= MY_UNPACK_FILENAME | MY_REPLACE_EXT;
  }
  fn_format(index_file_name, index_file_name_arg, mysql_data_home,
            ".index", opt);
  if ((index_file_nr= my_open(index_file_name,
                              O_RDWR | O_CREAT | O_BINARY ,
                              MYF(MY_WME))) < 0 ||
       my_sync(index_file_nr, MYF(MY_WME)) ||
       init_io_cache(&index_file, index_file_nr,
                     IO_SIZE, WRITE_CACHE,
                     my_seek(index_file_nr,0L,MY_SEEK_END,MYF(0)),
			0, MYF(MY_WME | MY_WAIT_IF_FULL)) ||
      DBUG_EVALUATE_IF("fault_injection_openning_index", 1, 0))
  {
    /*
      TODO: all operations creating/deleting the index file or a log, should
      call my_sync_dir() or my_sync_dir_by_file() to be durable.
      TODO: file creation should be done with my_create() not my_open().
    */
    if (index_file_nr >= 0)
      my_close(index_file_nr,MYF(0));
    return 1;
  }

#ifdef HAVE_REPLICATION
  /*
    Sync the index by purging any binary log file that is not registered.
    In other words, either purge binary log files that were removed from
    the index but not purged from the file system due to a crash or purge
    any binary log file that was created but not register in the index
    due to a crash.
  */

  if (set_purge_index_file_name(index_file_name_arg) ||
      open_purge_index_file(FALSE) ||
      purge_index_entry(NULL, NULL, need_mutex) ||
      close_purge_index_file() ||
      DBUG_EVALUATE_IF("fault_injection_recovering_index", 1, 0))
  {
    sql_print_error("MYSQL_BIN_LOG::open_index_file failed to sync the index "
                    "file.");
    return 1;
  }
#endif

  return 0;
}


/**
  Read a line, which may or may not include a group_id, from the index file.

  @param[in,out]  log_name       Buffer to receive the full-path name of the
                                 log file, must be at least LOG_NAME_LEN in
                                 size
  @param[out]     length         Receives the actual length of string put into
                                 log_name, may be 0 or 1 if EOF reached
  @param[out]     group_id_arg   Recieves any group_id read from the file,
                                 or 0 if line in the file didn't contain one
  @param[out]     server_id_arg  Recieves any server_id read from the file,
                                 or 0 if line in the file didn't contain one

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::read_index_entry(char *log_name, uint *length,
                                    ulonglong *group_id_arg,
                                    uint32 *server_id_arg)
{
  /*
    Have to look for and parse group_ids out of the entry
    regardless of rpl_hierarchical because the option may
    have been set and then later turned off.
  */

  /* Initialize group_id_arg to default. */
  *group_id_arg= 0;
  *server_id_arg= 0;

  /* If we get 0 or 1 characters, this is the end of the file. */
  if (((*length= my_b_gets(&index_file, log_name, LOG_NAME_LEN)) <= 1) ||
      (log_name[(*length) - 1] != '\n'))
  {
    return 1;
  }

  if (log_name[(*length) - 2] == rpl_hierarchical_index_delimiter)
  {
    /*
      Only the bin log should have group_ids. At process initilization this
      code is called as part of the opening the TC_LOG to check if the
      previous shutdown was clean. In that case, log_type == LOG_UNKNOWN.
    */
    if (log_type != LOG_BIN && log_type != LOG_UNKNOWN)
    {
      sql_print_error("read_index_entry found group_id delimiter on"
                      "unexpected log type(%d).", log_type);
      index_file.error= LOG_INFO_INVALID;
      return 1;
    }

    /*
      Line ends in the delimiter, so appears to have a group_id & server_id.
      Start searching back for the other delimiters. We can limit how far
      back up the string we'll search for the other delimiters before giving
      up based on max len of a ulonglong and uint32. The entry looks something
      like this:

      /export/hda3/mysql/slave-relay-bin.000002|328983|1755911|\n
    */
    int stop_pos= max(0, ((int) (*length)) - (LOG_NAME_LEN - FN_REFLEN));
    int i= (*length) - 3;
    int server_id_delim= -1;
    for ( ; ; i--)
    {
      if (i < stop_pos)
      {
        /* Should have found the delimiter already. File is invalid. */
        log_name[(*length) - 1]= '\0';
        sql_print_error("read_index_entry failed to find group_id delimiter "
                        "parsing line: %s", log_name);
        index_file.error= LOG_INFO_INVALID;
        return 1;
      }

      if (log_name[i] == rpl_hierarchical_index_delimiter)
      {
        if (server_id_delim < 0)
          /* Found first delimiter. */
          server_id_delim= i;
        else
          /* Found both delimiters. Exit loop. */
          break;
      }
    }

    DBUG_ASSERT(i > 0 && server_id_delim > 0);

    char *end_char;
    *group_id_arg= strtoull(log_name + i + 1, &end_char, 10);

    /*
      end_char should now be pointing to the delimiter between the group_id
      and the server_id.
    */
    if (end_char != log_name + server_id_delim)
    {
      sql_print_error("read_index_entry consumed incorrect number of "
                      "characters reading group_id.");
      index_file.error= LOG_INFO_INVALID;
      return 1;
    }

    *server_id_arg= strtoul(log_name + server_id_delim + 1, &end_char, 10);

    /* end_char should now be pointing to the terminating delimiter. */
    if (end_char != (log_name + *length - 2))
    {
      sql_print_error("read_index_entry consumed incorrect number of "
                      "characters reading server_id.");
      index_file.error= LOG_INFO_INVALID;
      return 1;
    }

    /*
      Doctor string back to format callers expected before the addition
      of group_ids.
    */
    log_name[i]= '\n';
    *length= i + 1;
  }

  return 0;
}

/**
  Write a line to the index file which may or may not include a group_id.

  @param  log_name       full-path name of log file to write to the file
  @param  group_id_arg   the group_id to also write to the line if
                         rpl_hierarchical and the log is a bin log,
                         not a relay log
  @param  server_id_arg  the server_id to also write to the line if
                         rpl_hierarchical and the log is a bin log,
                         not a relay log
  @param  from_recover   if set, forces writing the group_id and server_id
                         despite the log_type not being LOG_BIN

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::write_index_entry(char *log_name, ulonglong group_id_arg,
                                     uint32 server_id_arg, bool from_recover)
{
  if (my_b_write(&index_file, (uchar *) log_name, strlen(log_name)))
  {
    sql_print_error("MYSQL_LOG::write_index_entry failed to write to index "
                    "file. log_name = %s.", log_name);
    return 1;
  }

  if (rpl_hierarchical && (log_type == LOG_BIN || from_recover))
  {
    char llbuf[22];

    /*
      The entry will look something like this:

      /export/hda3/mysql/slave-relay-bin.000002|328983|1755911|\n
    */
    /*
       Not using llstr as it takes a longlong, not a ulonglong, so results
       in the index file aren't what one would expect if the group_id climbs
       high enough that the high bit is set.
    */
    /* llstr((longlong) group_id_arg, llbuf); */
    int len= snprintf(llbuf, 22, "%llu", group_id_arg);
    if (len < 1 || len > 22)
    {
      sql_print_error("MYSQL_LOG::write_index_entry, snprintf failed "
                      "returning %d", len);
      return 1;
    }
    if (my_b_write(&index_file,
                   (uchar *) &rpl_hierarchical_index_delimiter, 1) ||
        my_b_write(&index_file, (uchar *) llbuf, strlen(llbuf)) ||
        my_b_write(&index_file, (uchar *) &rpl_hierarchical_index_delimiter, 1))
    {
      sql_print_error("MYSQL_LOG::write_index_entry failed to write to index "
                      "file. group_id = %s", llbuf);
      return 1;
    }

    len= snprintf(llbuf, 11, "%u", server_id_arg);
    if (len < 1 || len > 11)
    {
      sql_print_error("MYSQL_LOG::write_index_entry, snprintf failed "
                      "returning %d", len);
      return 1;
    }
    if (my_b_write(&index_file, (uchar *) llbuf, strlen(llbuf)) ||
        my_b_write(&index_file, (uchar *) &rpl_hierarchical_index_delimiter, 1))
    {
      sql_print_error("MYSQL_LOG::write_index_entry failed to write to index "
                      "file. server_id = %s", llbuf);
      return 1;
    }
  }

  if (my_b_write(&index_file, (uchar *) "\n", 1))
  {
    sql_print_error("MYSQL_LOG::write_index_entry failed to write \\n to "
                    "index file.");
    return 1;
  }

  return 0;
}

/**
  Updates the last entry in the index file to have last consumed group_id.

  @param  log_name      Log name which should have its group_id updated
  @param  need_lock     Set if parent does not have a lock on LOCK_index
  @param  from_recover  Set if called from recovery code where log_type is
                        not yet set to LOG_BIN

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::write_group_id_to_index(char *log_name,
                                           bool need_lock, bool from_recover)
{
  /* Check if we even need to do any work. */
  if (!rpl_hierarchical)
    return 0;

  /*
     Only want to write to the index for the bin log, but during recover
     log_type == LOG_CLOSED so we pass in a flag and force the code to
     run in that case.
  */
  if (!from_recover && log_type != LOG_BIN)
    return 0;

  LOG_INFO log_info;
  int error;

  if ((error= find_log_pos(&log_info, log_name, need_lock)))
  {
    sql_print_error("MYSQL_LOG::write_group_id_to_index call to "
                    "find_log_pos() failed with error = %d)", error);
    return 1;
  }

  DBUG_ASSERT(my_b_inited(&index_file) != 0);
  if (reinit_io_cache(&index_file, WRITE_CACHE,
                      log_info.index_file_start_offset, 0, 0) ||
      write_index_entry(log_info.log_file_name, group_id, last_event_server_id,
                        from_recover) ||
      my_chsize(index_file.file, my_b_tell(&index_file), '\n',  MYF(MY_WME)) ||
      flush_io_cache(&index_file) ||
      my_sync(index_file.file, MYF(MY_WME)))
  {
    sql_print_error("MYSQL_LOG::write_group_id_to_index failed to update "
                    "index file. log_info.index_file_start_offset = %lu ,"
                    "log_info.log_file_name = %s",
                    (unsigned long) log_info.index_file_start_offset,
                    log_info.log_file_name);
    return 1;
  }

  /*
    NOTE: Some threads cache a LOG_INFO, but since we only wrote to the last
    line of the index file we don't need to do any adjustments since LOG_INFO
    now only stores index_file_start_offset, which is still valid with the
    write just done above.
  */

  return 0;
}


/**
  Open a (new) binlog file.

  - Open the log file and the index file. Register the new
  file name in it
  - When calling this when the file is in use, you must have a locks
  on LOCK_log and LOCK_index.

  @retval
    0	ok
  @retval
    1	error
*/

bool MYSQL_BIN_LOG::open(const char *log_name,
                         enum_log_type log_type_arg,
                         const char *new_name,
                         enum cache_type io_cache_type_arg,
                         bool no_auto_events_arg,
                         ulong max_size_arg,
                         bool null_created_arg,
                         bool need_mutex)
{
  File file= -1;

  DBUG_ENTER("MYSQL_BIN_LOG::open");
  DBUG_PRINT("enter",("log_type: %d",(int) log_type_arg));

  if (init_and_set_log_file_name(log_name, new_name, log_type_arg,
                                 io_cache_type_arg))
  {
    sql_print_error("MSYQL_BIN_LOG::open failed to generate new file name.");
    DBUG_RETURN(1);
  }

#ifdef HAVE_REPLICATION
  if (open_purge_index_file(TRUE) ||
      register_create_index_entry(log_file_name) ||
      sync_purge_index_file() ||
      DBUG_EVALUATE_IF("fault_injection_registering_index", 1, 0))
  {
    /**
        TODO: although this was introduced to appease valgrind
              when injecting emulated faults using fault_injection_registering_index
              it may be good to consider what actually happens when
              open_purge_index_file succeeds but register or sync fails.

              Perhaps we might need the code below in MYSQL_LOG_BIN::cleanup
              for "real life" purposes as well? 
     */
    DBUG_EXECUTE_IF("fault_injection_registering_index", {
      if (my_b_inited(&purge_index_file))
      {
        end_io_cache(&purge_index_file);
        my_close(purge_index_file.file, MYF(0));
      }
    });

    sql_print_error("MSYQL_BIN_LOG::open failed to sync the index file.");
    DBUG_RETURN(1);
  }
  DBUG_EXECUTE_IF("crash_create_non_critical_before_update_index", DBUG_SUICIDE(););
#endif

  write_error= 0;

  /* open the main log file */
  if (MYSQL_LOG::open(log_name, log_type_arg, new_name,
                      io_cache_type_arg))
  {
#ifdef HAVE_REPLICATION
    close_purge_index_file();
#endif
    DBUG_RETURN(1);                            /* all warnings issued */
  }

  init(no_auto_events_arg, max_size_arg);

  open_count++;

  DBUG_ASSERT(log_type == LOG_BIN || log_type == LOG_RELAY);

  {
    bool write_file_name_to_index_file=0;

    if (!my_b_filelength(&log_file))
    {
      /*
	The binary log file was empty (probably newly created)
	This is the normal case and happens when the user doesn't specify
	an extension for the binary log files.
	In this case we write a standard header to it.
      */
      if (my_b_safe_write(&log_file, (uchar*) BINLOG_MAGIC,
			  BIN_LOG_HEADER_SIZE))
        goto err;
      bytes_written+= BIN_LOG_HEADER_SIZE;
      write_file_name_to_index_file= 1;
    }

    if (!no_auto_events)
    {
      Format_description_log_event s(BINLOG_VERSION);
      /*
        don't set LOG_EVENT_BINLOG_IN_USE_F for SEQ_READ_APPEND io_cache
        as we won't be able to reset it later
      */
      if (io_cache_type == WRITE_CACHE)
        s.flags|= LOG_EVENT_BINLOG_IN_USE_F;
      if (rpl_event_checksums)
        s.flags|= LOG_EVENT_EVENTS_HAVE_CRC_F;
      if (!s.is_valid())
        goto err;
      s.dont_set_created= null_created_arg;
      if (log_type == LOG_RELAY)
        s.set_relay_log_event();
      if (s.write(&log_file, false /* only_checksum_body */))
        goto err;
      bytes_written+= s.data_written;
    }
    if (description_event_for_queue &&
        description_event_for_queue->binlog_version>=4)
    {
      /*
        This is a relay log written to by the I/O slave thread.
        Write the event so that others can later know the format of this relay
        log.
        Note that this event is very close to the original event from the
        master (it has binlog version of the master, event types of the
        master), so this is suitable to parse the next relay log's event. It
        has been produced by
        Format_description_log_event::Format_description_log_event(char* buf,).
        Why don't we want to write the description_event_for_queue if this
        event is for format<4 (3.23 or 4.x): this is because in that case, the
        description_event_for_queue describes the data received from the
        master, but not the data written to the relay log (*conversion*),
        which is in format 4 (slave's).
      */
      /*
        Set 'created' to 0, so that in next relay logs this event does not
        trigger cleaning actions on the slave in
        Format_description_log_event::apply_event_impl().
      */
      description_event_for_queue->created= 0;
      /* Don't set log_pos in event header */
      description_event_for_queue->set_artificial_event();

      if (description_event_for_queue->write(&log_file,
                                             false /* only_checksum_body */))
        goto err;
      bytes_written+= description_event_for_queue->data_written;
    }
    if (flush_io_cache(&log_file) ||
        my_sync(log_file.file, MYF(MY_WME)))
      goto err;

    if (write_file_name_to_index_file)
    {
#ifdef HAVE_REPLICATION
      DBUG_EXECUTE_IF("crash_create_critical_before_update_index", DBUG_SUICIDE(););
#endif

      DBUG_ASSERT(my_b_inited(&index_file) != 0);
      reinit_io_cache(&index_file, WRITE_CACHE,
                      my_b_filelength(&index_file), 0, 0);
      /*
        As this is a new log file, we write the file name to the index
        file. As every time we write to the index file, we sync it.
      */
      if (DBUG_EVALUATE_IF("fault_injection_updating_index", 1, 0) ||
          write_index_entry(log_file_name, group_id, last_event_server_id,
                            false) ||
          flush_io_cache(&index_file) ||
          my_sync(index_file.file, MYF(MY_WME)))
        goto err;

#ifdef HAVE_REPLICATION
      DBUG_EXECUTE_IF("crash_create_after_update_index", DBUG_SUICIDE(););
#endif

      if (rpl_hierarchical && rpl_hier_cache_frequency_real &&
          log_type == LOG_BIN)
        repl_hier_cache_append_file(group_id, last_event_server_id,
                                    log_file_name);
    }
  }
  log_state= LOG_OPENED;

#ifdef HAVE_REPLICATION
  close_purge_index_file();
#endif

  DBUG_RETURN(0);

err:
#ifdef HAVE_REPLICATION
  if (is_inited_purge_index_file())
    purge_index_entry(NULL, NULL, need_mutex);
  close_purge_index_file();
#endif
  sql_print_error("Could not use %s for logging (error %d). \
Turning logging off for the whole duration of the MySQL server process. \
To turn it on again: fix the cause, \
shutdown the MySQL server and restart it.", name, errno);
  if (file >= 0)
    my_close(file,MYF(0));
  end_io_cache(&log_file);
  end_io_cache(&index_file);
  safeFree(name);
  log_state= LOG_CLOSED;
  DBUG_RETURN(1);
}


int MYSQL_BIN_LOG::get_current_log(LOG_INFO* linfo)
{
  pthread_mutex_lock(&LOCK_log);
  int ret = raw_get_current_log(linfo);
  pthread_mutex_unlock(&LOCK_log);
  return ret;
}

int MYSQL_BIN_LOG::raw_get_current_log(LOG_INFO* linfo)
{
  strmake(linfo->log_file_name, log_file_name, sizeof(linfo->log_file_name)-1);
  linfo->pos = my_b_tell(&log_file);
  linfo->group_id= group_id;
  linfo->server_id= last_event_server_id;
  return 0;
}

/**
  Move all data up in a file in an filename index file.

    We do the copy outside of the IO_CACHE as the cache buffers would just
    make things slower and more complicated.
    In most cases the copy loop should only do one read.

  @param index_file			File to move
  @param offset			Move everything from here to beginning

  @note
    File will be truncated to be 'offset' shorter or filled up with newlines

  @retval
    0	ok
*/

#ifdef HAVE_REPLICATION

static bool copy_up_file_and_fill(IO_CACHE *index_file, my_off_t offset)
{
  int bytes_read;
  my_off_t init_offset= offset;
  File file= index_file->file;
  uchar io_buf[IO_SIZE*2];
  DBUG_ENTER("copy_up_file_and_fill");

  for (;; offset+= bytes_read)
  {
    (void) my_seek(file, offset, MY_SEEK_SET, MYF(0));
    if ((bytes_read= (int) my_read(file, io_buf, sizeof(io_buf), MYF(MY_WME)))
	< 0)
      goto err;
    if (!bytes_read)
      break;					// end of file
    (void) my_seek(file, offset-init_offset, MY_SEEK_SET, MYF(0));
    if (my_write(file, io_buf, bytes_read, MYF(MY_WME | MY_NABP)))
      goto err;
  }
  /* The following will either truncate the file or fill the end with \n' */
  if (my_chsize(file, offset - init_offset, '\n', MYF(MY_WME)) ||
      my_sync(file, MYF(MY_WME)))
    goto err;

  /* Reset data in old index cache */
  reinit_io_cache(index_file, READ_CACHE, (my_off_t) 0, 0, 1);
  DBUG_RETURN(0);

err:
  DBUG_RETURN(1);
}

#endif /* HAVE_REPLICATION */

/**
  Find the position in the log-index-file for the given log name.

  @param linfo		Store here the found log file name and position to
                       the NEXT log file name in the index file.
  @param log_name	Filename to find in the index file.
                       Is a null pointer if we want to read the first entry
  @param need_lock	Set this to 1 if the parent doesn't already have a
                       lock on LOCK_index

  @note
    On systems without the truncate function the file will end with one or
    more empty lines.  These will be ignored when reading the file.

  @retval
    0			ok
  @retval
    LOG_INFO_EOF	        End of log-index-file found
  @retval
    LOG_INFO_IO		Got IO error while reading file
*/

int MYSQL_BIN_LOG::find_log_pos(LOG_INFO *linfo, const char *log_name,
			    bool need_lock)
{
  int error= 0;
  char *fname= linfo->log_file_name;
  uint log_name_len= log_name ? (uint) strlen(log_name) : 0;
  DBUG_ENTER("find_log_pos");
  DBUG_PRINT("enter",("log_name: %s", log_name ? log_name : "NULL"));

  /*
    Mutex needed because we need to make sure the file pointer does not
    move from under our feet
  */
  if (need_lock)
    pthread_mutex_lock(&LOCK_index);
  safe_mutex_assert_owner(&LOCK_index);

  /* As the file is flushed, we can't get an error here */
  (void) reinit_io_cache(&index_file, READ_CACHE, (my_off_t) 0, 0, 0);

  for (;;)
  {
    uint length;
    my_off_t offset= my_b_tell(&index_file);

    if (read_index_entry(fname, &length, &linfo->group_id, &linfo->server_id))
    {
      /* Did not find the given entry; Return not found or error */
      error= !index_file.error ? LOG_INFO_EOF : LOG_INFO_IO;
      break;
    }

    // if the log entry matches, null string matching anything
    if (!log_name ||
	(log_name_len == length-1 && fname[log_name_len] == '\n' &&
	 !memcmp(fname, log_name, log_name_len)))
    {
      DBUG_PRINT("info",("Found log file entry"));
      fname[length-1]=0;			// remove last \n
      linfo->index_file_start_offset= offset;
      break;
    }
  }

  if (need_lock)
    pthread_mutex_unlock(&LOCK_index);
  DBUG_RETURN(error);
}


/**
  Find the position in the log-index-file for the given log name.

  @param
    linfo		Store here the next log file name and position to
			the file name after that.
  @param
    need_lock		Set this to 1 if the parent doesn't already have a
			lock on LOCK_index

  @note
    - Before calling this function, one has to call find_log_pos()
    to set up 'linfo'
    - Mutex needed because we need to make sure the file pointer does not move
    from under our feet

  @retval
    0			ok
  @retval
    LOG_INFO_EOF	        End of log-index-file found
  @retval
    LOG_INFO_IO		Got IO error while reading file
*/

int MYSQL_BIN_LOG::find_next_log(LOG_INFO* linfo, bool need_lock)
{
  int error= 0;
  uint length;
  char *fname= linfo->log_file_name;

  if (need_lock)
    pthread_mutex_lock(&LOCK_index);
  safe_mutex_assert_owner(&LOCK_index);

  /* As the file is flushed, we can't get an error here */
  if (reinit_io_cache(&index_file, READ_CACHE,
                      linfo->index_file_start_offset, 0, 0))
  {
    sql_print_error("MYSQL_LOG::find_next_log got failure from "
                    "reinit_io_cache.");
    error= LOG_INFO_IO;
    goto err;
  }

  /*
    When the bin log is closed, the index may be updated with the group_id
    of the last event in the log, altering the length of the "current" line.
    Thus, we read the "current line" again before moving on to the next.
  */
  if ((length= my_b_gets(&index_file, fname, LOG_NAME_LEN)) <= 1)
  {
    sql_print_error("MYSQL_LOG::find_next_log got failure from "
                    "my_b_gets.");
    error= LOG_INFO_INVALID;
    static const uint *null_ptr= NULL;
    if (*(null_ptr)) null_ptr = NULL;
    goto err;
  }

  DBUG_ASSERT(linfo->index_file_start_offset + length ==
              my_b_tell(&index_file));
  linfo->index_file_start_offset= my_b_tell(&index_file);
  if (read_index_entry(fname, &length, &linfo->group_id, &linfo->server_id))
  {
    error = !index_file.error ? LOG_INFO_EOF : LOG_INFO_IO;
    goto err;
  }
  fname[length-1]=0;				// kill \n

err:
  if (need_lock)
    pthread_mutex_unlock(&LOCK_index);
  return error;
}


/**
  Delete all logs refered to in the index file.
  Start writing to a new log file.

  The new index file will only contain this file.

  @param  thd        Thread, NULL when called from init_slave
  @param  need_lock  Set this to 1 if the parent doesn't already have a
                     lock on LOCK_index

  @note
    If not called from slave thread, write start event to new log

  @retval
    0	ok
  @retval
    1   error
*/

bool MYSQL_BIN_LOG::reset_logs(THD *thd, bool need_lock)
{
  LOG_INFO linfo;
  bool error=0;
  const char* save_name;
  bool do_semi_sync_reset_master= false;
  DBUG_ENTER("reset_logs");

  // If called from init_slave() it is just to purge relay logs so no
  // need to notify the storage engines.
  if (thd) ha_reset_logs(thd);

  /*
    The following mutex is needed to ensure that no threads call
    'delete thd' as we would then risk missing a 'rollback' from this
    thread. If the transaction involved MyISAM tables, it should go
    into binlog even on rollback.
  */
  pthread_mutex_lock(&LOCK_thread_count);

  if (need_lock)
  {
    /*
      We need to get both locks to be sure that no one is trying to
      write to the index log file.
    */
    pthread_mutex_lock(&LOCK_log);
    pthread_mutex_lock(&LOCK_index);
  }
  safe_mutex_assert_owner(&LOCK_log);
  safe_mutex_assert_owner(&LOCK_index);

  if (log_type == LOG_BIN)
  {
    do_semi_sync_reset_master= true;
    int result= semi_sync_replicator.reset_master(thd);
    if (result)
    {
      error= 1;
      goto err;
    }
  }

  /* Save variables so that we can reopen the log */
  save_name=name;
  name=0;					// Protect against free
  close(LOG_CLOSE_TO_BE_OPENED);

  /*
    First delete all old log files and then update the index file.
    As we first delete the log files and do not use sort of logging,
    a crash may lead to an inconsistent state where the index has
    references to non-existent files.

    We need to invert the steps and use the purge_index_file methods
    in order to make the operation safe.
  */
  if (find_log_pos(&linfo, NullS, 0))
  {
    error=1;
    goto err;
  }

  for (;;)
  {
    if ((error= my_delete_allow_opened(linfo.log_file_name, MYF(0))) != 0)
    {
      if (my_errno == ENOENT) 
      {
        if (thd)
          push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                              ER_LOG_PURGE_NO_FILE, ER(ER_LOG_PURGE_NO_FILE),
                              linfo.log_file_name);
        sql_print_information("Failed to delete file '%s'",
                              linfo.log_file_name);
        my_errno= 0;
        error= 0;
      }
      else
      {
        if (thd)
          push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                              ER_BINLOG_PURGE_FATAL_ERR,
                              "a problem with deleting %s; "
                              "consider examining correspondence "
                              "of your binlog index file "
                              "to the actual binlog files",
                              linfo.log_file_name);
        error= 1;
        goto err;
      }
    }
    if (find_next_log(&linfo, 0))
      break;
  }

  if (rpl_hierarchical && rpl_hier_cache_frequency_real &&
      log_type == LOG_BIN)
    repl_hier_cache_clear();

  /* Start logging with a new file */
  close(LOG_CLOSE_INDEX | LOG_CLOSE_TO_BE_OPENED);
  if ((error= my_delete_allow_opened(index_file_name, MYF(0))))	// Reset (open will update)
  {
    if (my_errno == ENOENT) 
    {
      if (thd)
        push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                            ER_LOG_PURGE_NO_FILE, ER(ER_LOG_PURGE_NO_FILE),
                            index_file_name);
      sql_print_information("Failed to delete file '%s'",
                            index_file_name);
      my_errno= 0;
      error= 0;
    }
    else
    {
      if (thd)
        push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                            ER_BINLOG_PURGE_FATAL_ERR,
                            "a problem with deleting %s; "
                            "consider examining correspondence "
                            "of your binlog index file "
                            "to the actual binlog files",
                            index_file_name);
      error= 1;
      goto err;
    }
  }
  if (!open_index_file(index_file_name, 0, FALSE))
    if ((error= open(save_name, log_type, 0, io_cache_type, no_auto_events, max_size, 0, FALSE)))
      goto err;
  my_free((uchar*) save_name, MYF(0));

err:
  if (do_semi_sync_reset_master)
  {
    semi_sync_replicator.reset_master_complete();
  }

  if (need_lock) {
    pthread_mutex_unlock(&LOCK_index);
    pthread_mutex_unlock(&LOCK_log);
  }
  VOID(pthread_mutex_unlock(&LOCK_thread_count));
  DBUG_RETURN(error);
}


/**
  Delete relay log files prior to rli->group_relay_log_name
  (i.e. all logs which are not involved in a non-finished group
  (transaction)), remove them from the index file and start on next
  relay log.

  IMPLEMENTATION
  - Protects index file with LOCK_index
  - Delete relevant relay log files
  - Copy all file names after these ones to the front of the index file
  - If the OS has truncate, truncate the file, else fill it with \n'
  - Read the next file name from the index file and store in rli->linfo

  @param rli	       Relay log information
  @param included     If false, all relay logs that are strictly before
                      rli->group_relay_log_name are deleted ; if true, the
                      latter is deleted too (i.e. all relay logs
                      read by the SQL slave thread are deleted).

  @note
    - This is only called from the slave-execute thread when it has read
    all commands from a relay log and want to switch to a new relay log.
    - When this happens, we can be in an active transaction as
    a transaction can span over two relay logs
    (although it is always written as a single block to the master's binary
    log, hence cannot span over two master's binary logs).

  @retval
    0			ok
  @retval
    LOG_INFO_EOF	        End of log-index-file found
  @retval
    LOG_INFO_SEEK	Could not allocate IO cache
  @retval
    LOG_INFO_IO		Got IO error while reading file
*/

#ifdef HAVE_REPLICATION

int MYSQL_BIN_LOG::purge_first_log(Relay_log_info* rli, bool included)
{
  int error;
  char *to_purge_if_included= NULL;
  DBUG_ENTER("purge_first_log");

  DBUG_ASSERT(is_open());
  DBUG_ASSERT(rli->slave_running == 1);
  DBUG_ASSERT(!strcmp(rli->linfo.log_file_name,rli->event_relay_log_name));

  pthread_mutex_lock(&LOCK_index);
  to_purge_if_included= my_strdup(rli->group_relay_log_name, MYF(0));

  /*
    Read the next log file name from the index file and pass it back to
    the caller.
  */
  if((error=find_log_pos(&rli->linfo, rli->event_relay_log_name, 0)) || 
     (error=find_next_log(&rli->linfo, 0)))
  {
    char buff[22];
    sql_print_error("next log error: %d  offset: %s  log: %s included: %d",
                    error,
                    llstr(rli->linfo.index_file_start_offset,buff),
                    rli->event_relay_log_name,
                    included);
    goto err;
  }

  /*
    Reset rli's coordinates to the current log.
  */
  rli->event_relay_log_pos= BIN_LOG_HEADER_SIZE;
  strmake(rli->event_relay_log_name,rli->linfo.log_file_name,
	  sizeof(rli->event_relay_log_name)-1);

  /*
    If we removed the rli->group_relay_log_name file,
    we must update the rli->group* coordinates, otherwise do not touch it as the
    group's execution is not finished (e.g. COMMIT not executed)
  */
  if (included)
  {
    rli->group_relay_log_pos = BIN_LOG_HEADER_SIZE;
    strmake(rli->group_relay_log_name,rli->linfo.log_file_name,
            sizeof(rli->group_relay_log_name)-1);
    rli->notify_group_relay_log_name_update();
  }

  /* Store where we are in the new file for the execution thread */
  flush_relay_log_info(rli);

  DBUG_EXECUTE_IF("crash_before_purge_logs", DBUG_SUICIDE(););

  pthread_mutex_lock(&rli->log_space_lock);
  rli->relay_log.purge_logs(to_purge_if_included, included,
                            0, 0, &rli->log_space_total);
  // Tell the I/O thread to take the relay_log_space_limit into account
  rli->ignore_log_space_limit= 0;
  pthread_mutex_unlock(&rli->log_space_lock);

  /*
    Ok to broadcast after the critical region as there is no risk of
    the mutex being destroyed by this thread later - this helps save
    context switches
  */
  pthread_cond_broadcast(&rli->log_space_cond);

  /*
   * Need to update the log pos because purge logs has been called 
   * after fetching initially the log pos at the begining of the method.
   */
  if((error=find_log_pos(&rli->linfo, rli->event_relay_log_name, 0)))
  {
    char buff[22];
    sql_print_error("next log error: %d  offset: %s  log: %s included: %d",
                    error,
                    llstr(rli->linfo.index_file_start_offset, buff),
                    rli->group_relay_log_name,
                    included);
    goto err;
  }

  /* If included was passed, rli->linfo should be the first entry. */
  DBUG_ASSERT(!included || rli->linfo.index_file_start_offset == 0);

err:
  my_free(to_purge_if_included, MYF(0));
  pthread_mutex_unlock(&LOCK_index);
  DBUG_RETURN(error);
}

/**
  Update log index_file.
*/

int MYSQL_BIN_LOG::update_log_index(LOG_INFO* log_info, bool need_update_threads)
{
  if (copy_up_file_and_fill(&index_file, log_info->index_file_start_offset))
    return LOG_INFO_IO;

  // now update offsets in index file for running threads
  if (need_update_threads)
    adjust_linfo_offsets(log_info->index_file_start_offset);
  return 0;
}

/**
  Remove all logs before the given log from disk and from the index file.

  @param to_log	      Delete all log file name before this file.
  @param included            If true, to_log is deleted too.
  @param need_mutex
  @param need_update_threads If we want to update the log coordinates of
                             all threads. False for relay logs, true otherwise.
  @param freed_log_space     If not null, decrement this variable of
                             the amount of log space freed

  @note
    If any of the logs before the deleted one is in use,
    only purge logs up to this one.

  @retval
    0			ok
  @retval
    LOG_INFO_EOF		to_log not found
    LOG_INFO_EMFILE             too many files opened
    LOG_INFO_FATAL              if any other than ENOENT error from
                                my_stat() or my_delete()
*/

int MYSQL_BIN_LOG::purge_logs(const char *to_log, 
                          bool included,
                          bool need_mutex, 
                          bool need_update_threads, 
                          ulonglong *decrease_log_space)
{
  int error= 0;
  bool exit_loop= 0;
  LOG_INFO log_info;
  THD *thd= current_thd;
  DBUG_ENTER("purge_logs");
  DBUG_PRINT("info",("to_log= %s",to_log));

  if (need_mutex)
    pthread_mutex_lock(&LOCK_index);
  if ((error=find_log_pos(&log_info, to_log, 0 /*no mutex*/))) 
  {
    sql_print_error("MYSQL_BIN_LOG::purge_logs was called with file %s not "
                    "listed in the index.", to_log);
    goto err;
  }

  if ((error= open_purge_index_file(TRUE)))
  {
    sql_print_error("MYSQL_BIN_LOG::purge_logs failed to sync the index file.");
    goto err;
  }

  /*
    File name exists in index file; delete until we find this file
    or a file that is used.
  */
  if ((error=find_log_pos(&log_info, NullS, 0 /*no mutex*/)))
    goto err;
  while ((strcmp(to_log,log_info.log_file_name) || (exit_loop=included)) &&
         !is_active(log_info.log_file_name) &&
         !log_in_use(log_info.log_file_name))
  {
    if ((error= register_purge_index_entry(log_info.log_file_name)))
    {
      sql_print_error("MYSQL_BIN_LOG::purge_logs failed to copy %s to register file.",
                      log_info.log_file_name);
      goto err;
    }

    if (rpl_hierarchical && rpl_hier_cache_frequency_real &&
        log_type == LOG_BIN)
      repl_hier_cache_purge_file(log_info.log_file_name);

    if (find_next_log(&log_info, 0) || exit_loop)
      break;
  }

  DBUG_EXECUTE_IF("crash_purge_before_update_index", DBUG_SUICIDE(););

  if ((error= sync_purge_index_file()))
  {
    sql_print_error("MSYQL_BIN_LOG::purge_logs failed to flush register file.");
    goto err;
  }

  /* We know how many files to delete. Update index file. */
  if ((error=update_log_index(&log_info, need_update_threads)))
  {
    sql_print_error("MSYQL_BIN_LOG::purge_logs failed to update the index file");
    goto err;
  }

  DBUG_EXECUTE_IF("crash_purge_critical_after_update_index", DBUG_SUICIDE(););

err:
  /* Read each entry from purge_index_file and delete the file. */
  if (is_inited_purge_index_file() &&
      (error= purge_index_entry(thd, decrease_log_space, FALSE)))
    sql_print_error("MSYQL_BIN_LOG::purge_logs failed to process registered files"
                    " that would be purged.");
  close_purge_index_file();

  DBUG_EXECUTE_IF("crash_purge_non_critical_after_update_index", DBUG_SUICIDE(););

  if (need_mutex)
    pthread_mutex_unlock(&LOCK_index);
  DBUG_RETURN(error);
}

int MYSQL_BIN_LOG::set_purge_index_file_name(const char *base_file_name)
{
  int error= 0;
  DBUG_ENTER("MYSQL_BIN_LOG::set_purge_index_file_name");
  if (fn_format(purge_index_file_name, base_file_name, mysql_data_home,
                ".~rec~", MYF(MY_UNPACK_FILENAME | MY_SAFE_PATH |
                              MY_REPLACE_EXT)) == NULL)
  {
    error= 1;
    sql_print_error("MYSQL_BIN_LOG::set_purge_index_file_name failed to set "
                      "file name.");
  }
  DBUG_RETURN(error);
}

int MYSQL_BIN_LOG::open_purge_index_file(bool destroy)
{
  int error= 0;
  File file= -1;

  DBUG_ENTER("MYSQL_BIN_LOG::open_purge_index_file");

  if (destroy)
    close_purge_index_file();

  if (!my_b_inited(&purge_index_file))
  {
    if ((file= my_open(purge_index_file_name, O_RDWR | O_CREAT | O_BINARY,
                       MYF(MY_WME | ME_WAITTANG))) < 0  ||
        init_io_cache(&purge_index_file, file, IO_SIZE,
                      (destroy ? WRITE_CACHE : READ_CACHE),
                      0, 0, MYF(MY_WME | MY_NABP | MY_WAIT_IF_FULL)))
    {
      error= 1;
      sql_print_error("MYSQL_BIN_LOG::open_purge_index_file failed to open register "
                      " file.");
    }
  }
  DBUG_RETURN(error);
}

int MYSQL_BIN_LOG::close_purge_index_file()
{
  int error= 0;

  DBUG_ENTER("MYSQL_BIN_LOG::close_purge_index_file");

  if (my_b_inited(&purge_index_file))
  {
    end_io_cache(&purge_index_file);
    error= my_close(purge_index_file.file, MYF(0));
  }
  my_delete(purge_index_file_name, MYF(0));
  bzero((char*) &purge_index_file, sizeof(purge_index_file));

  DBUG_RETURN(error);
}

bool MYSQL_BIN_LOG::is_inited_purge_index_file()
{
  DBUG_ENTER("MYSQL_BIN_LOG::is_inited_purge_index_file");
  DBUG_RETURN (my_b_inited(&purge_index_file));
}

int MYSQL_BIN_LOG::sync_purge_index_file()
{
  int error= 0;
  DBUG_ENTER("MYSQL_BIN_LOG::sync_purge_index_file");

  if ((error= flush_io_cache(&purge_index_file)) ||
      (error= my_sync(purge_index_file.file, MYF(MY_WME))))
    DBUG_RETURN(error);

  DBUG_RETURN(error);
}

int MYSQL_BIN_LOG::register_purge_index_entry(const char *entry)
{
  int error= 0;
  DBUG_ENTER("MYSQL_BIN_LOG::register_purge_index_entry");

  if ((error=my_b_write(&purge_index_file, (const uchar*)entry, strlen(entry))) ||
      (error=my_b_write(&purge_index_file, (const uchar*)"\n", 1)))
    DBUG_RETURN (error);

  DBUG_RETURN(error);
}

int MYSQL_BIN_LOG::register_create_index_entry(const char *entry)
{
  DBUG_ENTER("MYSQL_BIN_LOG::register_create_index_entry");
  DBUG_RETURN(register_purge_index_entry(entry));
}

int MYSQL_BIN_LOG::purge_index_entry(THD *thd, ulonglong *decrease_log_space,
                                     bool need_mutex)
{
  MY_STAT s;
  int error= 0;
  LOG_INFO log_info;
  LOG_INFO check_log_info;

  DBUG_ENTER("MYSQL_BIN_LOG:purge_index_entry");

  DBUG_ASSERT(my_b_inited(&purge_index_file));

  if ((error=reinit_io_cache(&purge_index_file, READ_CACHE, 0, 0, 0)))
  {
    sql_print_error("MSYQL_BIN_LOG::purge_index_entry failed to reinit register file "
                    "for read");
    goto err;
  }

  for (;;)
  {
    uint length;

    if ((length=my_b_gets(&purge_index_file, log_info.log_file_name,
                          FN_REFLEN)) <= 1)
    {
      if (purge_index_file.error)
      {
        error= purge_index_file.error;
        sql_print_error("MSYQL_BIN_LOG::purge_index_entry error %d reading from "
                        "register file.", error);
        goto err;
      }

      /* Reached EOF */
      break;
    }

    /* Get rid of the trailing '\n' */
    log_info.log_file_name[length-1]= 0;

    if (!my_stat(log_info.log_file_name, &s, MYF(0)))
    {
      if (my_errno == ENOENT) 
      {
        /*
          It's not fatal if we can't stat a log file that does not exist;
          If we could not stat, we won't delete.
        */
        if (thd)
        {
          push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                              ER_LOG_PURGE_NO_FILE, ER(ER_LOG_PURGE_NO_FILE),
                              log_info.log_file_name);
        }
        sql_print_information("Failed to execute my_stat on file '%s'",
			      log_info.log_file_name);
        my_errno= 0;
      }
      else
      {
        /*
          Other than ENOENT are fatal
        */
        if (thd)
        {
          push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                              ER_BINLOG_PURGE_FATAL_ERR,
                              "a problem with getting info on being purged %s; "
                              "consider examining correspondence "
                              "of your binlog index file "
                              "to the actual binlog files",
                              log_info.log_file_name);
        }
        else
        {
          sql_print_information("Failed to delete log file '%s'; "
                                "consider examining correspondence "
                                "of your binlog index file "
                                "to the actual binlog files",
                                log_info.log_file_name);
        }
        error= LOG_INFO_FATAL;
        goto err;
      }
    }
    else
    {
      if ((error= find_log_pos(&check_log_info, log_info.log_file_name, need_mutex)))
      {
        if (error != LOG_INFO_EOF)
        {
          if (thd)
          {
            push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                                ER_BINLOG_PURGE_FATAL_ERR,
                                "a problem with deleting %s and "
                                "reading the binlog index file",
                                log_info.log_file_name);
          }
          else
          {
            sql_print_information("Failed to delete file '%s' and "
                                  "read the binlog index file",
                                  log_info.log_file_name);
          }
          goto err;
        }
           
        error= 0;
        if (!need_mutex)
        {
          /*
            This is to avoid triggering an error in NDB.
          */
          ha_binlog_index_purge_file(current_thd, log_info.log_file_name);
        }

        DBUG_PRINT("info",("purging %s",log_info.log_file_name));
        if (!my_delete(log_info.log_file_name, MYF(0)))
        {
          if (decrease_log_space)
            *decrease_log_space-= s.st_size;
        }
        else
        {
          if (my_errno == ENOENT)
          {
            if (thd)
            {
              push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                                  ER_LOG_PURGE_NO_FILE, ER(ER_LOG_PURGE_NO_FILE),
                                  log_info.log_file_name);
            }
            sql_print_information("Failed to delete file '%s'",
                                  log_info.log_file_name);
            my_errno= 0;
          }
          else
          {
            if (thd)
            {
              push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                                  ER_BINLOG_PURGE_FATAL_ERR,
                                  "a problem with deleting %s; "
                                  "consider examining correspondence "
                                  "of your binlog index file "
                                  "to the actual binlog files",
                                  log_info.log_file_name);
            }
            else
            {
              sql_print_information("Failed to delete file '%s'; "
                                    "consider examining correspondence "
                                    "of your binlog index file "
                                    "to the actual binlog files",
                                    log_info.log_file_name);
            }
            if (my_errno == EMFILE)
            {
              DBUG_PRINT("info",
                         ("my_errno: %d, set ret = LOG_INFO_EMFILE", my_errno));
              error= LOG_INFO_EMFILE;
              goto err;
            }
            error= LOG_INFO_FATAL;
            goto err;
          }
        }
      }
    }
  }

err:
  DBUG_RETURN(error);
}

/**
  Remove all logs before the given file date from disk and from the
  index file.

  @param thd		Thread pointer
  @param purge_time	Delete all log files before given date.

  @note
    If any of the logs before the deleted one is in use,
    only purge logs up to this one.

  @retval
    0				ok
  @retval
    LOG_INFO_PURGE_NO_ROTATE	Binary file that can't be rotated
    LOG_INFO_FATAL              if any other than ENOENT error from
                                my_stat() or my_delete()
*/

int MYSQL_BIN_LOG::purge_logs_before_date(time_t purge_time)
{
  int error;
  char to_log[FN_REFLEN];
  LOG_INFO log_info;
  MY_STAT stat_area;
  THD *thd= current_thd;
  
  DBUG_ENTER("purge_logs_before_date");

  pthread_mutex_lock(&LOCK_index);
  to_log[0]= 0;

  if ((error=find_log_pos(&log_info, NullS, 0 /*no mutex*/)))
    goto err;

  while (strcmp(log_file_name, log_info.log_file_name) &&
	 !is_active(log_info.log_file_name) &&
         !log_in_use(log_info.log_file_name))
  {
    if (!my_stat(log_info.log_file_name, &stat_area, MYF(0)))
    {
      if (my_errno == ENOENT) 
      {
        /*
          It's not fatal if we can't stat a log file that does not exist.
        */
        my_errno= 0;
      }
      else
      {
        /*
          Other than ENOENT are fatal
        */
        if (thd)
        {
          push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                              ER_BINLOG_PURGE_FATAL_ERR,
                              "a problem with getting info on being purged %s; "
                              "consider examining correspondence "
                              "of your binlog index file "
                              "to the actual binlog files",
                              log_info.log_file_name);
        }
        else
        {
          sql_print_information("Failed to delete log file '%s'",
                                log_info.log_file_name);
        }
        error= LOG_INFO_FATAL;
        goto err;
      }
    }
    else
    {
      if (stat_area.st_mtime < purge_time) 
        strmake(to_log, 
                log_info.log_file_name, 
                sizeof(log_info.log_file_name) - 1);
      else
        break;
    }
    if (find_next_log(&log_info, 0))
      break;
  }

  error= (to_log[0] ? purge_logs(to_log, 1, 0, 1, (ulonglong *) 0) : 0);

err:
  pthread_mutex_unlock(&LOCK_index);
  DBUG_RETURN(error);
}
#endif /* HAVE_REPLICATION */


/**
  Create a new log file name.

  @param buf		buf of at least FN_REFLEN where new name is stored

  @note
    If file name will be longer then FN_REFLEN it will be truncated
*/

void MYSQL_BIN_LOG::make_log_name(char* buf, const char* log_ident)
{
  uint dir_len = dirname_length(log_file_name); 
  if (dir_len >= FN_REFLEN)
    dir_len=FN_REFLEN-1;
  strnmov(buf, log_file_name, dir_len);
  strmake(buf+dir_len, log_ident, FN_REFLEN - dir_len -1);
}


/**
  Check if we are writing/reading to the given log file.
*/

bool MYSQL_BIN_LOG::is_active(const char *log_file_name_arg)
{
  return !strcmp(log_file_name, log_file_name_arg);
}


/*
  Wrappers around new_file_impl to avoid using argument
  to control locking. The argument 1) less readable 2) breaks
  incapsulation 3) allows external access to the class without
  a lock (which is not possible with private new_file_without_locking
  method).

  @retval
    nonzero - error
*/

int MYSQL_BIN_LOG::new_file()
{
  return new_file_impl(1);
}

/*
  @retval
    nonzero - error
 */
int MYSQL_BIN_LOG::new_file_without_locking()
{
  return new_file_impl(0);
}


/**
  Start writing to a new log file or reopen the old file.

  @param need_lock		Set to 1 if caller has not locked LOCK_log

  @retval
    nonzero - error

  @note
    The new file name is stored last in the index file
*/

int MYSQL_BIN_LOG::new_file_impl(bool need_lock)
{
  int error= 0, close_on_error= FALSE;
  char new_name[FN_REFLEN], *new_name_ptr, *old_name, *file_to_open;
  File duped_file= -1;

  DBUG_ENTER("MYSQL_BIN_LOG::new_file_impl");
  if (!is_open())
  {
    DBUG_PRINT("info",("log is closed"));
    DBUG_RETURN(error);
  }

  if (need_lock)
    pthread_mutex_lock(&LOCK_log);
  pthread_mutex_lock(&LOCK_index);

  safe_mutex_assert_owner(&LOCK_log);
  safe_mutex_assert_owner(&LOCK_index);

  /*
    if binlog is used as tc log, be sure all xids are "unlogged",
    so that on recover we only need to scan one - latest - binlog file
    for prepared xids. As this is expected to be a rare event,
    simple wait strategy is enough. We're locking LOCK_log to be sure no
    new Xid_log_event's are added to the log (and prepared_xids is not
    increased), and waiting on COND_prep_xids for late threads to
    catch up.
  */
  if (prepared_xids)
  {
    tc_log_page_waits++;
    pthread_mutex_lock(&LOCK_prep_xids);
    while (prepared_xids) {
      DBUG_PRINT("info", ("prepared_xids=%lu", prepared_xids));
      pthread_cond_wait(&COND_prep_xids, &LOCK_prep_xids);
    }
    pthread_mutex_unlock(&LOCK_prep_xids);
  }

  /* Reuse old name if not binlog and not update log */
  new_name_ptr= name;

  /*
    If user hasn't specified an extension, generate a new log name
    We have to do this here and not in open as we want to store the
    new file name in the current binary log file.
  */
  if ((error= generate_new_name(new_name, name)))
    goto end;
  new_name_ptr=new_name;

  if (log_type == LOG_BIN || log_type == LOG_RELAY)
  {
    /*
      Previously close() would clear the in-use bit but it has been changed
      to not do so because the in-use bit shouldn't get cleared until the
      rotate to a new log has been successful. Thus, if close() would have
      cleared the in-use bit, then we now need to do so here. open() and
      close() both check for WRITE_CACHE, which effectively means that only
      the bin log gets the bit set and then cleared.
    */
    if (log_file.type == WRITE_CACHE &&
        (duped_file= my_dup(log_file.file, MYF(MY_WME))) < 0)
    {
      sql_print_error("MYSQL_LOG::new_file failed to duplicate file desc.");
      goto end;
    }

    if (!no_auto_events)
    {
      /*
        We log the whole file name for log file as the user may decide
        to change base names at some point.
      */
      Rotate_log_event r(new_name+dirname_length(new_name),
                         0, LOG_EVENT_OFFSET, is_relay_log ? Rotate_log_event::RELAY_LOG : 0);
      if(DBUG_EVALUATE_IF("fault_injection_new_file_rotate_event", (error=close_on_error=TRUE), FALSE) ||
         (error= r.write(&log_file, false /* only_checksum_body */)))
      {
        DBUG_EXECUTE_IF("fault_injection_new_file_rotate_event", errno=2;);
        close_on_error= TRUE;
        my_printf_error(ER_ERROR_ON_WRITE, ER(ER_CANT_OPEN_FILE), MYF(ME_FATALERROR), name, errno);
        goto end;
      }
      bytes_written += r.data_written;
    }
    /*
      Update needs to be signalled even if there is no rotate event
      log rotation should give the waiting thread a signal to
      discover EOF and move on to the next log.
    */
    signal_update();
  }
  old_name=name;
  name=0;				// Don't free name
  close(LOG_CLOSE_TO_BE_OPENED | LOG_CLOSE_INDEX);

  /*
     Note that at this point, log_state != LOG_CLOSED (important for is_open()).
  */

  /*
     new_file() is only used for rotation (in FLUSH LOGS or because size >
     max_binlog_size or max_relay_log_size).
     If this is a binary log, the Format_description_log_event at the beginning of
     the new file should have created=0 (to distinguish with the
     Format_description_log_event written at server startup, which should
     trigger temp tables deletion on slaves.
  */

  /* reopen index binlog file, BUG#34582 */
  file_to_open= index_file_name;
  error= open_index_file(index_file_name, 0, FALSE);
  if (!error)
  {
    /* reopen the binary log file. */
    file_to_open= new_name_ptr;
    error= open(old_name, log_type, new_name_ptr, io_cache_type,
                no_auto_events, max_size, 1, FALSE);
  }

  /* handle reopening errors */
  if (error)
  {
    my_printf_error(ER_CANT_OPEN_FILE, ER(ER_CANT_OPEN_FILE), 
                    MYF(ME_FATALERROR), file_to_open, error);
    close_on_error= TRUE;
  }
  else if (duped_file >= 0)
  {
    /* clearing LOG_EVENT_BINLOG_IN_USE_F. */
    my_off_t offset= BIN_LOG_HEADER_SIZE + FLAGS_OFFSET;
    uchar flags= 0;
    if (my_pwrite(duped_file, &flags, 1, offset, MYF(MY_NABP)) ||
        my_sync(duped_file, MYF(MY_WME)) ||
        my_close(duped_file, MYF(MY_WME)))
    {
      /*
        Complain about the error, but nothing really to do in response. The
        in-use bit may have been left on, but there's already a new file
        anyway so the only effect of the stale bit is that mysqlbinlog will
        write output about it not being closed cleanly.
      */
      sql_print_error("MYSQL_LOG::new_file got failure while trying to "
                      "clear in-use bit of old log.");
    }
  }

  my_free(old_name,MYF(0));

end:

  if (error && close_on_error /* rotate or reopen failed */)
  {
    /* 
      Close whatever was left opened.

      We are keeping the behavior as it exists today, ie,
      we disable logging and move on (see: BUG#51014).

      TODO: as part of WL#1790 consider other approaches:
       - kill mysql (safety);
       - try multiple locations for opening a log file;
       - switch server to protected/readonly mode
       - ...
    */
    close(LOG_CLOSE_INDEX);
    sql_print_error("Could not open %s for logging (error %d). "
                     "Turning logging off for the whole duration "
                     "of the MySQL server process. To turn it on "
                     "again: fix the cause, shutdown the MySQL "
                     "server and restart it.", 
                     new_name_ptr, errno);
  }

  if (need_lock)
    pthread_mutex_unlock(&LOCK_log);
  pthread_mutex_unlock(&LOCK_index);

  DBUG_RETURN(error);
}


bool MYSQL_BIN_LOG::append(Log_event* ev)
{
  bool error = 0;
  pthread_mutex_lock(&LOCK_log);
  DBUG_ENTER("MYSQL_BIN_LOG::append");

  DBUG_ASSERT(log_file.type == SEQ_READ_APPEND);
  /*
    Log_event::write() is smart enough to use my_b_write() or
    my_b_append() depending on the kind of cache we have.
  */
  if (ev->write(&log_file, false /* only_checksum_body */))
  {
    error=1;
    goto err;
  }
  bytes_written+= ev->data_written;
  DBUG_PRINT("info",("max_size: %lu",max_size));
  if ((uint) my_b_append_tell(&log_file) > max_size)
    error= new_file_without_locking();
err:
  pthread_mutex_unlock(&LOCK_log);
  signal_update();				// Safe as we don't call close
  DBUG_RETURN(error);
}


bool MYSQL_BIN_LOG::appendv(const char* buf, uint len,...)
{
  bool error= 0;
  DBUG_ENTER("MYSQL_BIN_LOG::appendv");
  va_list(args);
  va_start(args,len);

  DBUG_ASSERT(log_file.type == SEQ_READ_APPEND);

  safe_mutex_assert_owner(&LOCK_log);
  do
  {
    if (my_b_append(&log_file,(uchar*) buf,len))
    {
      error= 1;
      goto err;
    }
    bytes_written += len;
  } while ((buf=va_arg(args,const char*)) && (len=va_arg(args,uint)));
  DBUG_PRINT("info",("max_size: %lu",max_size));
  if ((uint) my_b_append_tell(&log_file) > max_size)
    error= new_file_without_locking();
err:
  if (!error)
    signal_update();
  DBUG_RETURN(error);
}


bool MYSQL_BIN_LOG::flush_and_sync()
{
  int err=0, fd=log_file.file;
  safe_mutex_assert_owner(&LOCK_log);
  if (flush_io_cache(&log_file))
    return 1;
  if (++sync_binlog_counter >= sync_binlog_period && sync_binlog_period)
  {
    sync_binlog_counter= 0;
    err=my_sync(fd, MYF(MY_WME));
  }
  return err;
}

void MYSQL_BIN_LOG::start_union_events(THD *thd, query_id_t query_id_param)
{
  DBUG_ASSERT(!thd->binlog_evt_union.do_union);
  thd->binlog_evt_union.do_union= TRUE;
  thd->binlog_evt_union.unioned_events= FALSE;
  thd->binlog_evt_union.unioned_events_trans= FALSE;
  thd->binlog_evt_union.first_query_id= query_id_param;
}

void MYSQL_BIN_LOG::stop_union_events(THD *thd)
{
  DBUG_ASSERT(thd->binlog_evt_union.do_union);
  thd->binlog_evt_union.do_union= FALSE;
}

bool MYSQL_BIN_LOG::is_query_in_union(THD *thd, query_id_t query_id_param)
{
  return (thd->binlog_evt_union.do_union && 
          query_id_param >= thd->binlog_evt_union.first_query_id);
}

/**
  This function checks if a transaction, either a multi-statement
  or a single statement transaction is about to commit or not.

  @param thd The client thread that executed the current statement.
  @param all Committing a transaction (i.e. TRUE) or a statement
             (i.e. FALSE).
  @return
    @c true if committing a transaction, otherwise @c false.
*/
bool ending_trans(const THD* thd, const bool all)
{
  return (all || (!all && !(thd->options & 
                  (OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT))));
}

/**
  This function checks if a non-transactional table was updated by
  the current transaction.

  @param thd The client thread that executed the current statement.
  @return
    @c true if a non-transactional table was updated, @c false
    otherwise.
*/
bool trans_has_updated_non_trans_table(const THD* thd)
{
  return (thd->transaction.all.modified_non_trans_table ||
          thd->transaction.stmt.modified_non_trans_table);
}

/**
  This function checks if any statement was committed and cached.

  @param thd The client thread that executed the current statement.
  @param all Committing a transaction (i.e. TRUE) or a statement
             (i.e. FALSE).
  @return
    @c true if at a statement was committed and cached, @c false
    otherwise.
*/
bool trans_has_no_stmt_committed(const THD* thd, bool all)
{
  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);

  return (!all && !trx_data->at_least_one_stmt_committed);
}

/**
  This function checks if a non-transactional table was updated by the
  current statement.

  @param thd The client thread that executed the current statement.
  @return
    @c true if a non-transactional table was updated, @c false otherwise.
*/
bool stmt_has_updated_non_trans_table(const THD* thd)
{
  return (thd->transaction.stmt.modified_non_trans_table);
}

/*
  These functions are placed in this file since they need access to
  binlog_hton, which has internal linkage.
*/

int THD::binlog_setup_trx_data()
{
  DBUG_ENTER("THD::binlog_setup_trx_data");
  binlog_trx_data *trx_data=
    (binlog_trx_data*) thd_get_ha_data(this, binlog_hton);

  if (trx_data)
    DBUG_RETURN(0);                             // Already set up

  trx_data= (binlog_trx_data*) my_malloc(sizeof(binlog_trx_data), MYF(MY_ZEROFILL));
  if (!trx_data ||
      open_cached_file(&trx_data->trans_log, mysql_tmpdir,
                       LOG_PREFIX, binlog_cache_size, MYF(MY_WME)))
  {
    my_free((uchar*)trx_data, MYF(MY_ALLOW_ZERO_PTR));
    DBUG_RETURN(1);                      // Didn't manage to set it up
  }
  thd_set_ha_data(this, binlog_hton, trx_data);

  trx_data= new (thd_get_ha_data(this, binlog_hton)) binlog_trx_data;

  DBUG_RETURN(0);
}

/*
  Function to start a statement and optionally a transaction for the
  binary log.

  SYNOPSIS
    binlog_start_trans_and_stmt()

  DESCRIPTION

    This function does three things:
    - Start a transaction if not in autocommit mode or if a BEGIN
      statement has been seen.

    - Start a statement transaction to allow us to truncate the binary
      log.

    - Save the currrent binlog position so that we can roll back the
      statement by truncating the transaction log.

      We only update the saved position if the old one was undefined,
      the reason is that there are some cases (e.g., for CREATE-SELECT)
      where the position is saved twice (e.g., both in
      select_create::prepare() and THD::binlog_write_table_map()) , but
      we should use the first. This means that calls to this function
      can be used to start the statement before the first table map
      event, to include some extra events.
 */

void
THD::binlog_start_trans_and_stmt()
{
  binlog_trx_data *trx_data= (binlog_trx_data*) thd_get_ha_data(this, binlog_hton);
  DBUG_ENTER("binlog_start_trans_and_stmt");
  DBUG_PRINT("enter", ("trx_data: 0x%lx  trx_data->before_stmt_pos: %lu",
                       (long) trx_data,
                       (trx_data ? (ulong) trx_data->before_stmt_pos :
                        (ulong) 0)));

  if (trx_data == NULL ||
      trx_data->before_stmt_pos == MY_OFF_T_UNDEF)
  {
    this->binlog_set_stmt_begin();
    if (options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
      trans_register_ha(this, TRUE, binlog_hton);
    trans_register_ha(this, FALSE, binlog_hton);
    /*
      Mark statement transaction as read/write. We never start
      a binary log transaction and keep it read-only,
      therefore it's best to mark the transaction read/write just
      at the same time we start it.
      Not necessary to mark the normal transaction read/write
      since the statement-level flag will be propagated automatically
      inside ha_commit_trans.
    */
    ha_data[binlog_hton->slot].ha_info[0].set_trx_read_write();
  }
  DBUG_VOID_RETURN;
}

void THD::binlog_set_stmt_begin() {
  binlog_trx_data *trx_data=
    (binlog_trx_data*) thd_get_ha_data(this, binlog_hton);

  /*
    The call to binlog_trans_log_savepos() might create the trx_data
    structure, if it didn't exist before, so we save the position
    into an auto variable and then write it into the transaction
    data for the binary log (i.e., trx_data).
  */
  my_off_t pos= 0;
  binlog_trans_log_savepos(this, &pos);
  trx_data= (binlog_trx_data*) thd_get_ha_data(this, binlog_hton);
  trx_data->before_stmt_pos= pos;
}


/*
  Write a table map to the binary log.
 */

int THD::binlog_write_table_map(TABLE *table, bool is_trans)
{
  int error;
  DBUG_ENTER("THD::binlog_write_table_map");
  DBUG_PRINT("enter", ("table: 0x%lx  (%s: #%lu)",
                       (long) table, table->s->table_name.str,
                       table->s->table_map_id));

  /* Pre-conditions */
  DBUG_ASSERT(current_stmt_binlog_row_based && mysql_bin_log.is_open());
  DBUG_ASSERT(table->s->table_map_id != ULONG_MAX);

  Table_map_log_event
    the_event(this, table, table->s->table_map_id, is_trans);

  if (is_trans && binlog_table_maps == 0)
    binlog_start_trans_and_stmt();

  if ((error= mysql_bin_log.write(&the_event)))
    DBUG_RETURN(error);

  binlog_table_maps++;
  DBUG_RETURN(0);
}

Rows_log_event*
THD::binlog_get_pending_rows_event() const
{
  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(this, binlog_hton);
  /*
    This is less than ideal, but here's the story: If there is no
    trx_data, prepare_pending_rows_event() has never been called
    (since the trx_data is set up there). In that case, we just return
    NULL.
   */
  return trx_data ? trx_data->pending() : NULL;
}

void
THD::binlog_set_pending_rows_event(Rows_log_event* ev)
{
  if (thd_get_ha_data(this, binlog_hton) == NULL)
    binlog_setup_trx_data();

  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(this, binlog_hton);

  DBUG_ASSERT(trx_data);
  trx_data->set_pending(ev);
}


/**
  Remove the pending rows event, discarding any outstanding rows.

  If there is no pending rows event available, this is effectively a
  no-op.
 */
int
MYSQL_BIN_LOG::remove_pending_rows_event(THD *thd)
{
  DBUG_ENTER("MYSQL_BIN_LOG::remove_pending_rows_event");

  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);

  DBUG_ASSERT(trx_data);

  if (Rows_log_event* pending= trx_data->pending())
  {
    delete pending;
    trx_data->set_pending(NULL);
  }

  DBUG_RETURN(0);
}

/*
  Moves the last bunch of rows from the pending Rows event to the binlog
  (either cached binlog if transaction, or disk binlog). Sets a new pending
  event.
*/
int
MYSQL_BIN_LOG::flush_and_set_pending_rows_event(THD *thd,
                                                Rows_log_event* event)
{
  DBUG_ENTER("MYSQL_BIN_LOG::flush_and_set_pending_rows_event(event)");
  DBUG_ASSERT(mysql_bin_log.is_open());
  DBUG_PRINT("enter", ("event: 0x%lx", (long) event));

  int error= 0;

  binlog_trx_data *const trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);

  DBUG_ASSERT(trx_data);

  DBUG_PRINT("info", ("trx_data->pending(): 0x%lx", (long) trx_data->pending()));

  if (Rows_log_event* pending= trx_data->pending())
  {
    IO_CACHE *file= &log_file;

    /*
      Decide if we should write to the log file directly or to the
      transaction log.
    */
    if (pending->get_cache_stmt() || my_b_tell(&trx_data->trans_log))
      file= &trx_data->trans_log;

    /*
      If we are not writing to the log file directly, we could avoid
      locking the log.
    */
    pthread_mutex_lock(&LOCK_log);

    /*
      Write pending event to log file or transaction cache
    */
    if (pending->write(file, file != &log_file))
    {
      pthread_mutex_unlock(&LOCK_log);
      set_write_error(thd);
      DBUG_RETURN(1);
    }

    delete pending;

    if (file == &log_file)
    {
      error= flush_and_sync();
      if (!error)
      {
        signal_update();
        error= rotate_and_purge(RP_LOCK_LOG_IS_ALREADY_LOCKED);
      }
    }

    pthread_mutex_unlock(&LOCK_log);
  }

  thd->binlog_set_pending_rows_event(event);

  DBUG_RETURN(error);
}

/**
  Determines if the current event should be skipped from going in the bin log.

  If hierarchical replication is enabled and and this server doesn't have a
  master, log the event because it means that this server is the root of the
  replication topology. If this server does have a master, only log the
  event if it is coming from the replication SQL thread. This is because we
  want a hierarhical replication slave's bin log to only contain events which
  originated from the topology's root master, not accumulating events from
  intermediate levels in the topology from statements like 'CREATE TEMPORARY
  TABLE' which may be allowed to run on such servers.

  @param  thd  the current thd

  @return true if the event should be skipped, false otherwise
*/
bool MYSQL_BIN_LOG::should_skip_event(THD *thd)
{
  safe_mutex_assert_owner(&LOCK_log);
  return rpl_hierarchical && !thd->slave_thread && have_master;
}

/**
  Determine group_id to use for writing event to bin log.

  When about to write an event to the bin log, determine what group_id
  should be used.

  @param  thd  the current thd

  @return the group_id to use
*/
ulonglong MYSQL_BIN_LOG::get_group_id_to_use(THD *thd)
{
  safe_mutex_assert_owner(&LOCK_log);

  ulonglong group_id_to_use= 0;

  if (rpl_hierarchical)
  {
    if (!thd->slave_thread || rpl_hierarchical_act_as_root)
      /*
        We're the root master. Use the group_id from our internal state.
        Note that this ID may be a continuation from the last group
        this server issued. Or, it may be that this server was previously
        a slave and just now became the root master. In that case, the
        server is continuing the ID sequence at the point the old server
        left off.
      */
      group_id_to_use= group_id + 1;
    else
    {
      if (thd->master_has_group_ids)
        /*
          We're a slave and on the replication SQL thread. Preserve the
          ID from the originating server, which was passed in the THD.
        */
        group_id_to_use= thd->group_id;
      else
        /*
          The master wasn't running with rpl_hierarchical when the event
          being executed was generated. In that case, we always write 0
          as that value should never actually show up in a bin log.
        */
        group_id_to_use= 0;
    }

    /*
      If thd->group_id < this->group_id, then the event should have been
      blocked in exec_relay_log_event and replication stopped.

      Would be nice to assert that the values differ by at most 1, but
      the existence of the 'set binlog_group_id=' statement means that
      it can't be asserted.
    */
    DBUG_ASSERT(group_id <= group_id_to_use ||
                (thd->slave_thread && !thd->master_has_group_ids));
    if (group_id_to_use < group_id &&
        (!thd->slave_thread || thd->master_has_group_ids))
    {
      char llbuf1[22], llbuf2[22];
      snprintf(llbuf1, 22, "%llu",
               (unsigned long long) group_id_to_use);
      snprintf(llbuf2, 22, "%llu",
               (unsigned long long) group_id);
      sql_print_error("About to write bad group_id = %s to the bin log. "
                      "Server's current group_id = %s", llbuf1, llbuf2);
    }
  }

  return group_id_to_use;
}


/**
  Write an event to the binary log.
*/

bool MYSQL_BIN_LOG::write(Log_event *event_info)
{
  THD *thd= event_info->thd;
  bool reported_binlog_offset= false;
  bool error= 1;
  DBUG_ENTER("MYSQL_BIN_LOG::write(Log_event *)");

  if (thd->binlog_evt_union.do_union)
  {
    /*
      In Stored function; Remember that function call caused an update.
      We will log the function call to the binary log on function exit
    */
    thd->binlog_evt_union.unioned_events= TRUE;
    thd->binlog_evt_union.unioned_events_trans |= event_info->cache_stmt;
    DBUG_RETURN(0);
  }

  /*
    Flush the pending rows event to the transaction cache or to the
    log file.  Since this function potentially aquire the LOCK_log
    mutex, we do this before aquiring the LOCK_log mutex in this
    function.

    We only end the statement if we are in a top-level statement.  If
    we are inside a stored function, we do not end the statement since
    this will close all tables on the slave.
  */
  bool const end_stmt=
    thd->prelocked_mode && thd->lex->requires_prelocking();
  if (thd->binlog_flush_pending_rows_event(end_stmt))
    DBUG_RETURN(error);

  pthread_mutex_lock(&LOCK_log);

  /* Check to see if we should skip logging this event. */
  if (should_skip_event(thd))
  {
    pthread_mutex_unlock(&LOCK_log);
    DBUG_RETURN(0);
  }

  /*
     In most cases this is only called if 'is_open()' is true; in fact this is
     mostly called if is_open() *was* true a few instructions before, but it
     could have changed since.
  */
  if (likely(is_open()))
  {
    IO_CACHE *file= &log_file;
#ifdef HAVE_REPLICATION
    /*
      In the future we need to add to the following if tests like
      "do the involved tables match (to be implemented)
      binlog_[wild_]{do|ignore}_table?" (WL#1049)"
    */
    const char *local_db= event_info->get_db();
    if ((thd && !(thd->options & OPTION_BIN_LOG)) ||
	(thd->lex->sql_command != SQLCOM_ROLLBACK_TO_SAVEPOINT &&
         thd->lex->sql_command != SQLCOM_SAVEPOINT &&
         !binlog_filter->db_ok(local_db)))
    {
      VOID(pthread_mutex_unlock(&LOCK_log));
      DBUG_RETURN(0);
    }
#endif /* HAVE_REPLICATION */

    /* Figure out which group_id to use for this event. */
    ulonglong group_id_to_use= get_group_id_to_use(thd);

#if defined(USING_TRANSACTIONS) 
    /*
      Should we write to the binlog cache or to the binlog on disk?

      Write to the binlog cache if:
      1 - a transactional engine/table is updated (stmt_has_updated_trans_table == TRUE);
      2 - or the event asks for it (cache_stmt == TRUE);
      3 - or the cache is already not empty (meaning we're in a transaction;
      note that the present event could be about a non-transactional table, but
      still we need to write to the binlog cache in that case to handle updates
      to mixed trans/non-trans table types).
      
      Write to the binlog on disk if only a non-transactional engine is
      updated and:
      1 - the binlog cache is empty or;
      2 - --binlog-direct-non-transactional-updates is set and we are about to
      use the statement format. When using the row format (cache_stmt == TRUE).
    */
    if (opt_using_transactions && thd)
    {
      if (thd->binlog_setup_trx_data())
        goto err;

      binlog_trx_data *const trx_data=
        (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);
      IO_CACHE *trans_log= &trx_data->trans_log;
      my_off_t trans_log_pos= my_b_tell(trans_log);
      if (event_info->get_cache_stmt() || stmt_has_updated_trans_table(thd) ||
          (!thd->variables.binlog_direct_non_trans_update &&
            trans_log_pos != 0))
      {
        DBUG_PRINT("info", ("Using trans_log: cache: %d, trans_log_pos: %lu",
                            event_info->get_cache_stmt(),
                            (ulong) trans_log_pos));
        thd->binlog_start_trans_and_stmt();
        file= trans_log;
        /*
          For events going to the transaction cache log, always use
          0. We'll fixup the events with a real group_id later in
          MYSQL_LOG::write_cache.
        */
        group_id_to_use= 0;
      }
      /*
        TODO as Mats suggested, for all the cases above where we write to
        trans_log, it sounds unnecessary to lock LOCK_log. We should rather
        test first if we want to write to trans_log, and if not, lock
        LOCK_log.
      */
    }
#endif /* USING_TRANSACTIONS */
    DBUG_PRINT("info",("event type: %d",event_info->get_type_code()));

    /*
      No check for auto events flag here - this write method should
      never be called if auto-events are enabled
    */

    /*
      1. Write first log events which describe the 'run environment'
      of the SQL command
    */

    /*
      If row-based binlogging, Insert_id, Rand and other kind of "setting
      context" events are not needed.
    */
    if (thd)
    {
      if (!thd->current_stmt_binlog_row_based)
      {
        if (thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt)
        {
          Intvar_log_event e(thd,(uchar) LAST_INSERT_ID_EVENT,
                             thd->first_successful_insert_id_in_prev_stmt_for_binlog,
                             group_id_to_use);
          if (e.write(file, file != &log_file))
            goto err;
          if (file == &log_file)
            thd->binlog_bytes_written+= e.data_written;
        }
        if (thd->auto_inc_intervals_in_cur_stmt_for_binlog.nb_elements() > 0)
        {
          DBUG_PRINT("info",("number of auto_inc intervals: %u",
                             thd->auto_inc_intervals_in_cur_stmt_for_binlog.
                             nb_elements()));
          Intvar_log_event e(thd, (uchar) INSERT_ID_EVENT,
                             thd->auto_inc_intervals_in_cur_stmt_for_binlog.
                             minimum(), group_id_to_use);
          if (e.write(file, file != &log_file))
            goto err;
          if (file == &log_file)
            thd->binlog_bytes_written+= e.data_written;
        }
        if (thd->rand_used)
        {
          Rand_log_event e(thd,thd->rand_saved_seed1, thd->rand_saved_seed2,
                           group_id_to_use);
          if (e.write(file, file != &log_file))
            goto err;
          if (file == &log_file)
            thd->binlog_bytes_written+= e.data_written;
        }
        if (thd->user_var_events.elements)
        {
          for (uint i= 0; i < thd->user_var_events.elements; i++)
          {
            BINLOG_USER_VAR_EVENT *user_var_event;
            get_dynamic(&thd->user_var_events,(uchar*) &user_var_event, i);
            User_var_log_event e(thd, user_var_event->user_var_event->name.str,
                                 user_var_event->user_var_event->name.length,
                                 user_var_event->value,
                                 user_var_event->length,
                                 user_var_event->type,
                                 user_var_event->charset_number,
                                 group_id_to_use);
            if (e.write(file, file != &log_file))
              goto err;
            if (file == &log_file)
              thd->binlog_bytes_written+= e.data_written;
          }
        }
      }
    }

    /*
      Set the group_id. Needs to be done while holding LOCK_log,
      which is why it's here and not done when event_info is
      constructed.
    */
    event_info->group_id= group_id_to_use;

    /*
       Write the SQL command
     */

    if (event_info->write(file, file != &log_file) ||
        DBUG_EVALUATE_IF("injecting_fault_writing", 1, 0))
      goto err;

    if (file == &log_file) // we are writing to the real log (disk)
    {
      thd->binlog_bytes_written+= event_info->data_written;

      if (flush_and_sync())
	goto err;

      /*
        We're writing to the real log, so update group_id to match
        what we just wrote.
      */
      if (group_id_to_use)
      {
        group_id= group_id_to_use;
        last_event_server_id= event_info->server_id;

        /*
          If the cache is on and the group_id is a multiple of the cache
          frequency, append to the cache. We do not append to the cache
          if the group_id == 0 because that means this server is a slave
          replicating from a master which isn't generating IDs and the
          cache logic breaks down under those conditions.
        */
        if (rpl_hierarchical && rpl_hier_cache_frequency_real &&
            !(group_id % rpl_hier_cache_frequency_real))
          repl_hier_cache_push_back(group_id, last_event_server_id,
                                    log_file_name, my_b_safe_tell(&log_file));
      }

      if (opt_using_transactions &&
          !(thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
      {
        /*
          LOAD DATA INFILE in AUTOCOMMIT=1 mode writes to the binlog
          chunks also before it is successfully completed. We only report
          the binlog write and do the commit inside the transactional table
          handler if the log event type is appropriate.

          TODO(ehudm): jtolmer: This comment and supporting logic are copied
          from the 4.0 sources. Is it still applicable? I would think that we'd
          always report the binlog offset here. We always consume a group_id at
          this point in the code.
          TODO(ehudm): Add another test that tests the conditions above to see
          if we can get rid of the "if" below.
        */

        if (event_info->get_type_code() == QUERY_EVENT ||
            event_info->get_type_code() == EXEC_LOAD_EVENT)
        {
          if (semi_sync_replicator.report_binlog_offset(thd,
                                                        log_file_name,
                                                        file->pos_in_file))
            goto err;
          reported_binlog_offset= true;
        }
      }

      signal_update();
      if ((error= rotate_and_purge(RP_LOCK_LOG_IS_ALREADY_LOCKED)))
        goto err;
      
    }

    /* Add sqllog entry for DDL statement */
    error= mysql_sql_log.log_ddl(thd);

err:
    if (error)
      set_write_error(thd);
  }

  pthread_mutex_unlock(&LOCK_log);

  /* Trigger semi-sync wait if it is enabled and needed. */
  if (reported_binlog_offset &&
      semi_sync_replicator.commit_trx(thd))
    error= 1;

  DBUG_RETURN(error);
}


int error_log_print(enum loglevel level, const char *format,
                    va_list args)
{
  return logger.error_log_print(level, format, args);
}


bool slow_log_print(THD *thd, const char *query, uint query_length,
                    ulonglong current_utime)
{
  return logger.slow_log_print(thd, query, query_length, current_utime);
}


bool LOGGER::log_command(THD *thd, enum enum_server_command command)
{
#ifndef NO_EMBEDDED_ACCESS_CHECKS
  Security_context *sctx= thd->security_ctx;
#endif
  /*
    Log command if we have at least one log event handler enabled and want
    to log this king of commands
  */
  if (*general_log_handler_list && (what_to_log & (1L << (uint) command)))
  {
    if ((thd->options & OPTION_LOG_OFF)
#ifndef NO_EMBEDDED_ACCESS_CHECKS
         && (sctx->master_access & SUPER_ACL)
#endif
       )
    {
      /* No logging */
      return FALSE;
    }

    return TRUE;
  }

  return FALSE;
}


bool general_log_print(THD *thd, enum enum_server_command command,
                       const char *format, ...)
{
  va_list args;
  uint error= 0;

  /* Print the message to the buffer if we want to log this king of commands */
  if (! logger.log_command(thd, command))
    return FALSE;

  va_start(args, format);
  error= logger.general_log_print(thd, command, format, args);
  va_end(args);

  return error;
}

bool general_log_write(THD *thd, enum enum_server_command command,
                       const char *query, uint query_length)
{
  /* Write the message to the log if we want to log this king of commands */
  if (logger.log_command(thd, command))
    return logger.general_log_write(thd, command, query, query_length);

  return FALSE;
}

bool audit_log_print(THD *thd, enum enum_server_command command,
                     const char *format, ...)
{
  va_list args;
  uint error= 0;

  va_start(args, format);
  error= logger.audit_log_print(thd, command, format, args);
  va_end(args);

  return error;
}

bool audit_log_write(THD *thd, enum enum_server_command command,
                       const char *query, uint query_length)
{
  return logger.audit_log_write(thd, command, query, query_length);
}

/**
  @note
    If rotation fails, for instance the server was unable 
    to create a new log file, we still try to write an 
    incident event to the current log.

  @retval
    nonzero - error 
*/
int MYSQL_BIN_LOG::rotate_and_purge(uint flags)
{
  int error= 0;
  DBUG_ENTER("MYSQL_BIN_LOG::rotate_and_purge");
#ifdef HAVE_REPLICATION
  bool check_purge= false;
#endif
  if (!(flags & RP_LOCK_LOG_IS_ALREADY_LOCKED))
    pthread_mutex_lock(&LOCK_log);
  if ((flags & RP_FORCE_ROTATE) ||
      (my_b_tell(&log_file) >= (my_off_t) max_size))
  {
    if ((error= new_file_without_locking()))
      /** 
         Be conservative... There are possible lost events (eg, 
         failing to log the Execute_load_query_log_event
         on a LOAD DATA while using a non-transactional
         table)!

         We give it a shot and try to write an incident event anyway
         to the current log. 
      */
      if (!write_incident(current_thd, FALSE, NULL))
        flush_and_sync();

#ifdef HAVE_REPLICATION
    check_purge= true;
#endif
  }
  if (!(flags & RP_LOCK_LOG_IS_ALREADY_LOCKED))
    pthread_mutex_unlock(&LOCK_log);
#ifdef HAVE_REPLICATION
  /*
    NOTE: Run purge_logs wo/ holding LOCK_log
          as it otherwise will deadlock in ndbcluster_binlog_index_purge_file
  */
  if (!error && check_purge && expire_logs_days)
  {
    time_t purge_time= my_time(0) - expire_logs_days*24*60*60;
    if (purge_time >= 0)
      purge_logs_before_date(purge_time);
  }
#endif
  DBUG_RETURN(error);
}

uint MYSQL_BIN_LOG::next_file_id()
{
  uint res;
  pthread_mutex_lock(&LOCK_log);
  res = file_id++;
  pthread_mutex_unlock(&LOCK_log);
  return res;
}


/*
  Helper function for write_cache. header must be at least
  LOG_EVENT_HEADER_LEN.
*/
void MYSQL_BIN_LOG::fix_header_checksum(uchar *header)
{
  ulong body_len= uint4korr(&header[EVENT_LEN_OFFSET]) -
      LOG_EVENT_HEADER_LEN;
  uint32 body_crc= uint4korr(&header[LOG_EVENT_HEADER_LEN -
                                     LOG_EVENT_CRC_LEN]);

  uint32 crc= my_checksum(0L, NULL, 0);

  /*
    In this case, the body checksum was computed when the event
    was writen to the transaction cache. However, the header
    checksum was not computed then because it would have become
    incorrect when the header data was updated in function write_cache
    where it did 'fix end_log_pos' and 'fix group_id', a few lines before
    the call to this function. Now that the header is in the final form
    which will show up in the bin log, compute its checksum and combine
    with the body checksum which was already computed.

    When computing the checksum for the event, we skip the checksum field
    itself.

    Checksum in the header always comes last. Thus, it doesn't have
    a fixed offset in the header as the offset depends on whether
    IDs are also being added.
  */
  crc= my_checksum(crc, header, LOG_EVENT_HEADER_LEN -
                   LOG_EVENT_CRC_LEN);
  crc= my_checksum_combine(crc, body_crc, body_len);
  int4store(&header[LOG_EVENT_HEADER_LEN - LOG_EVENT_CRC_LEN], crc);
}


/*
  Write the contents of a cache to the binary log.

  SYNOPSIS
    write_cache()
    cache    Cache to write to the binary log
    lock_log True if the LOCK_log mutex should be aquired, false otherwise
    sync_log True if the log should be flushed and sync:ed

  DESCRIPTION
    Write the contents of the cache to the binary log. The cache will
    be reset as a READ_CACHE to be able to read the contents from it.
 */

int MYSQL_BIN_LOG::write_cache(IO_CACHE *cache, bool lock_log, bool sync_log,
                               ulonglong group_id_arg)
{
  Mutex_sentry sentry(lock_log ? &LOCK_log : NULL);

  if (reinit_io_cache(cache, READ_CACHE, 0, 0, 0))
    return ER_ERROR_ON_WRITE;
  uint length= my_b_bytes_in_cache(cache), group, carry, hdr_offs;
  long val;
  uchar header[LOG_EVENT_HEADER_LEN];

  DBUG_EXECUTE_IF("half_binlogged_transaction", length-= 100;);

  /*
    The events in the buffer have incorrect end_log_pos data
    (relative to beginning of group rather than absolute),
    so we'll recalculate them in situ so the binlog is always
    correct, even in the middle of a group. This is possible
    because we now know the start position of the group (the
    offset of this cache in the log, if you will); all we need
    to do is to find all event-headers, and add the position of
    the group to the end_log_pos of each event.  This is pretty
    straight forward, except that we read the cache in segments,
    so an event-header might end up on the cache-border and get
    split.
  */

  group= (uint)my_b_tell(&log_file);
  hdr_offs= carry= 0;

  do
  {

    /*
      if we only got a partial header in the last iteration,
      get the other half now and process a full header.
    */
    if (unlikely(carry > 0))
    {
      DBUG_ASSERT(carry < LOG_EVENT_HEADER_LEN);

      /* assemble both halves */
      memcpy(&header[carry], (char *)cache->read_pos, LOG_EVENT_HEADER_LEN - carry);

      /* fix end_log_pos */
      val= uint4korr(&header[LOG_POS_OFFSET]) + group;
      int4store(&header[LOG_POS_OFFSET], val);

      /* fix group_id */
      if (rpl_hierarchical)
        int8store(&header[GROUP_ID_OFFSET], group_id_arg);

      if (rpl_event_checksums)
        fix_header_checksum(header);

      /* write the first half of the split header */
      if (my_b_write(&log_file, header, carry))
        return ER_ERROR_ON_WRITE;

      /*
        copy fixed second half of header to cache so the correct
        version will be written later.
      */
      memcpy((char *)cache->read_pos, &header[carry], LOG_EVENT_HEADER_LEN - carry);

      /* next event header at ... */
      hdr_offs = uint4korr(&header[EVENT_LEN_OFFSET]) - carry;

      carry= 0;
    }

    /* if there is anything to write, process it. */

    if (likely(length > 0))
    {
      /*
        process all event-headers in this (partial) cache.
        if next header is beyond current read-buffer,
        we'll get it later (though not necessarily in the
        very next iteration, just "eventually").
      */

      while (hdr_offs < length)
      {
        /*
          partial header only? save what we can get, process once
          we get the rest.
        */

        if (hdr_offs + LOG_EVENT_HEADER_LEN > length)
        {
          carry= length - hdr_offs;
          memcpy(header, (char *)cache->read_pos + hdr_offs, carry);
          length= hdr_offs;
        }
        else
        {
          /* we've got a full event-header, and it came in one piece */

          uchar *log_pos= (uchar *)cache->read_pos + hdr_offs + LOG_POS_OFFSET;

          /* fix end_log_pos */
          val= uint4korr(log_pos) + group;
          int4store(log_pos, val);

          /* fix group_id */
          if (rpl_hierarchical)
            int8store(cache->read_pos + hdr_offs + GROUP_ID_OFFSET,
                      group_id_arg);

          if (rpl_event_checksums)
            fix_header_checksum(cache->read_pos + hdr_offs);

          /* next event header at ... */
          log_pos= (uchar *)cache->read_pos + hdr_offs + EVENT_LEN_OFFSET;
          hdr_offs += uint4korr(log_pos);

        }
      }

      /*
        Adjust hdr_offs. Note that it may still point beyond the segment
        read in the next iteration; if the current event is very long,
        it may take a couple of read-iterations (and subsequent adjustments
        of hdr_offs) for it to point into the then-current segment.
        If we have a split header (!carry), hdr_offs will be set at the
        beginning of the next iteration, overwriting the value we set here:
      */
      hdr_offs -= length;
    }

    /* Write data to the binary log file */
    if (my_b_write(&log_file, cache->read_pos, length))
      return ER_ERROR_ON_WRITE;
    cache->read_pos=cache->read_end;		// Mark buffer used up
  } while ((length= my_b_fill(cache)));

  DBUG_ASSERT(carry == 0);

  if (sync_log)
    flush_and_sync();

  return 0;                                     // All OK
}

/*
  Helper function to get the error code of the query to be binlogged.
 */
int query_error_code(THD *thd, bool not_killed)
{
  int error;
  
  if (not_killed || (thd->killed == THD::KILL_BAD_DATA))
  {
    error= thd->is_error() ? thd->main_da.sql_errno() : 0;

    /* thd->main_da.sql_errno() might be ER_SERVER_SHUTDOWN or
       ER_QUERY_INTERRUPTED, So here we need to make sure that error
       is not set to these errors when specified not_killed by the
       caller.
    */
    if (error == ER_SERVER_SHUTDOWN || error == ER_QUERY_INTERRUPTED)
      error= 0;
  }
  else
  {
    /* killed status for DELAYED INSERT thread should never be used */
    DBUG_ASSERT(!(thd->system_thread & SYSTEM_THREAD_DELAYED_INSERT));
    error= thd->killed_errno();
  }

  return error;
}

bool MYSQL_BIN_LOG::write_incident(THD *thd, bool lock,
                                   ulonglong *group_id_to_use)
{
  uint error= 0;
  DBUG_ENTER("MYSQL_BIN_LOG::write_incident");

  if (!is_open())
    DBUG_RETURN(error);

  LEX_STRING const write_error_msg=
    { C_STRING_WITH_LEN("error writing to the binary log") };
  Incident incident= INCIDENT_LOST_EVENTS;
  Incident_log_event ev(thd, incident, write_error_msg);
  if (lock)
    pthread_mutex_lock(&LOCK_log);

  if (group_id_to_use == NULL)
    ev.group_id= get_group_id_to_use(thd);
  else
    ev.group_id= *group_id_to_use;

  error= ev.write(&log_file, false /* only_checksum_body */);
  thd->binlog_bytes_written+= ev.data_written;

  // Consume the group_id.
  if (group_id_to_use == NULL && !error)
    group_id= ev.group_id;

  if (lock)
  {
    if (!error && !(error= flush_and_sync()))
    {
      signal_update();
      error= rotate_and_purge(RP_LOCK_LOG_IS_ALREADY_LOCKED);
    }
    pthread_mutex_unlock(&LOCK_log);
  }
  DBUG_RETURN(error);
}

/**
  Write a cached log entry to the binary log.
  - To support transaction over replication, we wrap the transaction
  with BEGIN/COMMIT or BEGIN/ROLLBACK in the binary log.
  We want to write a BEGIN/ROLLBACK block when a non-transactional table
  was updated in a transaction which was rolled back. This is to ensure
  that the same updates are run on the slave.

  @param thd
  @param cache		The cache to copy to the binlog
  @param commit_event   The commit event to print after writing the
                        contents of the cache.
  @param incident       Defines if an incident event should be created to
                        notify that some non-transactional changes did
                        not get into the binlog.

  @note
    We only come here if there is something in the cache.
  @note
    The thing in the cache is always a complete transaction.
  @note
    'cache' needs to be reinitialized after this functions returns.
*/

bool MYSQL_BIN_LOG::write(THD *thd, IO_CACHE *cache, Log_event *commit_event,
                          bool incident)
{
  DBUG_ENTER("MYSQL_BIN_LOG::write(THD *, IO_CACHE *, Log_event *)");
  VOID(pthread_mutex_lock(&LOCK_log));

  /* Check to see if we should skip logging this event. */
  if (should_skip_event(thd))
  {
    pthread_mutex_unlock(&LOCK_log);
    DBUG_RETURN(0);
  }

  /* NULL would represent nothing to replicate after ROLLBACK */
  DBUG_ASSERT(commit_event != NULL);

  DBUG_ASSERT(is_open());
  if (likely(is_open()))                       // Should always be true
  {
    /*
      We only bother to write to the binary log if there is anything
      to write.
     */
    if (my_b_tell(cache) > 0)
    {
      /* Figure out which group_id to use for this event. */
      ulonglong group_id_to_use= get_group_id_to_use(thd);

      /*
        Log "BEGIN" at the beginning of every transaction.  Here, a
        transaction is either a BEGIN..COMMIT block or a single
        statement in autocommit mode.
      */
      Query_log_event qinfo(thd, STRING_WITH_LEN("BEGIN"), TRUE, TRUE, 0);

      /*
        Set the event's group_id. Above constructor used more than just here
        so chose to not change it and instead set the group_id here.
      */
      qinfo.group_id= group_id_to_use;

      /*
        Now this Query_log_event has artificial log_pos 0. It must be
        adjusted to reflect the real position in the log. Not doing it
        would confuse the slave: it would prevent this one from
        knowing where he is in the master's binlog, which would result
        in wrong positions being shown to the user, MASTER_POS_WAIT
        undue waiting etc.
      */
      if (qinfo.write(&log_file, false /* only_checksum_body */))
        goto err;
      thd->binlog_bytes_written+= qinfo.data_written;

      DBUG_EXECUTE_IF("crash_before_writing_xid",
                      {
                        if ((write_error= write_cache(cache, false, true,
                                                      group_id_to_use)))
                          DBUG_PRINT("info", ("error writing binlog cache: %d",
                                               write_error));
                        DBUG_PRINT("info", ("crashing before writing xid"));
                        DBUG_SUICIDE();
                      });

      if ((write_error= write_cache(cache, false, false, group_id_to_use)))
        goto err;
      thd->binlog_bytes_written+= my_b_tell(cache);
      DBUG_EXECUTE_IF("half_binlogged_transaction", goto DBUG_skip_commit;);

      if (commit_event)
      {
        /*
          Set the group_id. Needs to be done while holding LOCK_log,
          which is why it's here and not done when event_info is
          constructed.
        */
        commit_event->group_id= group_id_to_use;

        if (commit_event->write(&log_file, false /* only_checksum_body */))
          goto err;
        thd->binlog_bytes_written+= commit_event->data_written;
      }

      if (incident && write_incident(thd, FALSE, &group_id_to_use))
        goto err;

#ifndef DBUG_OFF
DBUG_skip_commit:
#endif
      if (flush_and_sync())
        goto err;
      DBUG_EXECUTE_IF("half_binlogged_transaction", DBUG_SUICIDE(););
      if (cache->error)				// Error on read
      {
        sql_print_error(ER(ER_ERROR_ON_READ), cache->file_name, errno);
        write_error=1;				// Don't give more errors
        goto err;
      }

      /*
        We're writing to the real log, so update group_id to match
        what we just wrote.
      */
      if (group_id_to_use)
      {
        group_id= group_id_to_use;
        last_event_server_id= commit_event->server_id;

        /*
          If the cache is on and the group_id is a multiple of the cache
          frequency, append to the cache. We do not append to the cache
          if the group_id == 0 because that means this server is a slave
          replicating from a master which isn't generating IDs and the
          cache logic breaks down under those conditions.
      */
        if (rpl_hierarchical && rpl_hier_cache_frequency_real &&
            !(group_id % rpl_hier_cache_frequency_real))
          repl_hier_cache_push_back(group_id, last_event_server_id,
                                    log_file_name, my_b_safe_tell(&log_file));
      }

      if (semi_sync_replicator.report_binlog_offset(thd,
                                                    log_file_name,
                                                    log_file.pos_in_file))
        goto err;

      signal_update();
    }

    /*
      if commit_event is Xid_log_event, increase the number of
      prepared_xids (it's decreasd in ::unlog()). Binlog cannot be rotated
      if there're prepared xids in it - see the comment in new_file() for
      an explanation.
      If the commit_event is not Xid_log_event (then it's a Query_log_event)
      rotate binlog, if necessary.
    */
    if (commit_event && commit_event->get_type_code() == XID_EVENT)
    {
      pthread_mutex_lock(&LOCK_prep_xids);
      prepared_xids++;
      pthread_mutex_unlock(&LOCK_prep_xids);
    }
    else
      if (rotate_and_purge(RP_LOCK_LOG_IS_ALREADY_LOCKED))
        goto err;
  }
  VOID(pthread_mutex_unlock(&LOCK_log));

  DBUG_RETURN(0);

err:
  if (!write_error)
  {
    write_error= 1;
    sql_print_error(ER(ER_ERROR_ON_WRITE), name, errno);
  }
  VOID(pthread_mutex_unlock(&LOCK_log));
  DBUG_RETURN(1);
}


/**
  Wait until we get a signal that the binary log has been updated.

  @param thd		Thread variable
  @param is_slave      If 0, the caller is the Binlog_dump thread from master;
                       if 1, the caller is the SQL thread from the slave. This
                       influences only thd->proc_info.

  @note
    One must have a lock on LOCK_log before calling this function.
    This lock will be released before return! That's required by
    THD::enter_cond() (see NOTES in sql_class.h).
*/

void MYSQL_BIN_LOG::wait_for_update(THD* thd, bool is_slave)
{
  const char *old_msg;
  DBUG_ENTER("wait_for_update");

  old_msg= thd->enter_cond(&update_cond, &LOCK_log,
                           is_slave ?
                           "Has read all relay log; waiting for the slave I/O "
                           "thread to update it" :
                           "Has sent all binlog to slave; waiting for binlog "
                           "to be updated");
  pthread_cond_wait(&update_cond, &LOCK_log);
  thd->exit_cond(old_msg);
  DBUG_VOID_RETURN;
}


/**
  Close the log file.

  @param exiting     Bitmask for one or more of the following bits:
          - LOG_CLOSE_INDEX : if we should close the index file
          - LOG_CLOSE_TO_BE_OPENED : if we intend to call open
                                     at once after close.
          - LOG_CLOSE_STOP_EVENT : write a 'stop' event to the log

  @note
    One can do an open on the object at once after doing a close.
    The internal structures are not freed until cleanup() is called
*/

void MYSQL_BIN_LOG::close(uint exiting)
{					// One can't set log_type here!
  DBUG_ENTER("MYSQL_BIN_LOG::close");
  DBUG_PRINT("enter",("exiting: %d", (int) exiting));
  if (log_state == LOG_OPENED)
  {
#ifdef HAVE_REPLICATION
    /*
      Used to issue a Stop_log_event here. That was removed for multiple
      reasons.

      1) The slave takes no action in response to the event
         so it isn't needed to begin with.
      2) If the master and slave are running with different binlog formats,
         the slave writes the stop event in its format while the rest of the
         events in the relay logs are of the master's format. This can cause
         the SQL thread to explode when it tries to read the stop event.
      3) If the master is generating an ID sequence we do not want the
         stop event to consume an ID.
    */
#endif /* HAVE_REPLICATION */

    /*
      Update the rpl_hier cache so that it knows the last group_id written
      to this log.
    */
    if (rpl_hierarchical && rpl_hier_cache_frequency_real &&
        log_type == LOG_BIN)
      repl_hier_cache_update_last_entry(group_id, last_event_server_id,
                                        log_file_name);

    /* don't pwrite in a file opened with O_APPEND - it doesn't work */
    if (log_file.type == WRITE_CACHE &&
        (log_type == LOG_BIN || log_type == LOG_RELAY))
    {
      /*
        Flush the last used group_id to the index file before clearing
        the IN_USE bit. Will do no work if !rpl_hierarchical. If
        LOG_CLOSE_STOP_EVENT is set, it means we're in the shut down
        case and write_group_id_to_index needs to take the lock. In
        the rotate to new bin log case the lock is already owned (which
        will be asserted down inside of find_log_pos).
      */
      if (write_group_id_to_index(log_file_name,
                                  exiting & LOG_CLOSE_STOP_EVENT, false))
      {
        /*
           Failed to update the index file with the group_id so leave bin
           log marked as in use. That way, if the bin log ends up being
           most recent on next server start, we'll recover the group_id
           from it.
        */
        sql_print_error("failed to update index with correct group_id");
      }
      /*
        If doing a rotate, do not clear the in-use bit now. new_file() will
        clear it after the new file is opened. Thus, if a crash occurs
        between now and the new file being successfully opened, the server
        is still able to detect an unclean shutdown.

        Note that LOG_CLOSE_TO_BE_OPENED is also passed from reset_logs(). It
        doesn't matter in that case if the bit is cleared because the file
        is going to get deleted right after anyway.
      */
      else if (!(exiting & LOG_CLOSE_TO_BE_OPENED))
      {
        /* clearing LOG_EVENT_BINLOG_IN_USE_F. */
        my_off_t offset= BIN_LOG_HEADER_SIZE + FLAGS_OFFSET;
        my_off_t org_position= my_tell(log_file.file, MYF(0));
        uchar flags= 0;
        if (my_pwrite(log_file.file, &flags, 1, offset, MYF(MY_NABP)))
        {
          sql_print_error("MYSQL_LOG::close got failure from my_pwrite "
                          "while trying to clear in-use bit.");
        }
        /*
          Restore position so that anything we have in the IO_cache is written
          to the correct position.
          We need the seek here, as my_pwrite() is not guaranteed to keep the
          original position on system that doesn't support pwrite().
        */
        my_seek(log_file.file, org_position, MY_SEEK_SET, MYF(0));
      }
    }

    /* this will cleanup IO_CACHE, sync and close the file */
    MYSQL_LOG::close(exiting);
  }

  /*
    The following test is needed even if is_open() is not set, as we may have
    called a not complete close earlier and the index file is still open.
  */

  if ((exiting & LOG_CLOSE_INDEX) && my_b_inited(&index_file))
  {
    end_io_cache(&index_file);
    if (my_close(index_file.file, MYF(0)) < 0 && ! write_error)
    {
      write_error= 1;
      sql_print_error(ER(ER_ERROR_ON_WRITE), index_file_name, errno);
    }
  }
  log_state= (exiting & LOG_CLOSE_TO_BE_OPENED) ? LOG_TO_BE_OPENED : LOG_CLOSED;
  safeFree(name);
  DBUG_VOID_RETURN;
}


void MYSQL_BIN_LOG::set_max_size(ulong max_size_arg)
{
  /*
    We need to take locks, otherwise this may happen:
    new_file() is called, calls open(old_max_size), then before open() starts,
    set_max_size() sets max_size to max_size_arg, then open() starts and
    uses the old_max_size argument, so max_size_arg has been overwritten and
    it's like if the SET command was never run.
  */
  DBUG_ENTER("MYSQL_BIN_LOG::set_max_size");
  pthread_mutex_lock(&LOCK_log);
  if (is_open())
    max_size= max_size_arg;
  pthread_mutex_unlock(&LOCK_log);
  DBUG_VOID_RETURN;
}


/**
  Check if a string is a valid number.

  @param str			String to test
  @param res			Store value here
  @param allow_wildcards	Set to 1 if we should ignore '%' and '_'

  @note
    For the moment the allow_wildcards argument is not used
    Should be move to some other file.

  @retval
    1	String is a number
  @retval
    0	Error
*/

static bool test_if_number(register const char *str,
			   long *res, bool allow_wildcards)
{
  reg2 int flag;
  const char *start;
  DBUG_ENTER("test_if_number");

  flag=0; start=str;
  while (*str++ == ' ') ;
  if (*--str == '-' || *str == '+')
    str++;
  while (my_isdigit(files_charset_info,*str) ||
	 (allow_wildcards && (*str == wild_many || *str == wild_one)))
  {
    flag=1;
    str++;
  }
  if (*str == '.')
  {
    for (str++ ;
	 my_isdigit(files_charset_info,*str) ||
	   (allow_wildcards && (*str == wild_many || *str == wild_one)) ;
	 str++, flag=1) ;
  }
  if (*str != 0 || flag == 0)
    DBUG_RETURN(0);
  if (res)
    *res=atol(start);
  DBUG_RETURN(1);			/* Number ok */
} /* test_if_number */


void sql_perror(const char *message)
{
#ifdef HAVE_STRERROR
  sql_print_error("%s: %s",message, strerror(errno));
#else
  perror(message);
#endif
}


/*
  Change the file associated with two output streams. Used to
  redirect stdout and stderr to a file. The streams are reopened
  only for appending (writing at end of file).
*/
extern "C" my_bool reopen_fstreams(const char *filename,
                                   FILE *outstream, FILE *errstream)
{
  if (outstream && !my_freopen(filename, "a", outstream))
    return TRUE;

  if (errstream && !my_freopen(filename, "a", errstream))
    return TRUE;

  /* The error stream must be unbuffered. */
  if (errstream)
    setbuf(errstream, NULL);

  return FALSE;
}


/*
  Unfortunately, there seems to be no good way
  to restore the original streams upon failure.
*/
static bool redirect_std_streams(const char *file)
{
  if (reopen_fstreams(file, stdout, stderr))
    return TRUE;

  setbuf(stderr, NULL);
  return FALSE;
}


bool flush_error_log()
{
  bool result= 0;
  if (opt_error_log)
  {
    VOID(pthread_mutex_lock(&LOCK_error_log));
    if (redirect_std_streams(log_error_file))
      result= 1;
    VOID(pthread_mutex_unlock(&LOCK_error_log));
  }
  return result;
}

void MYSQL_BIN_LOG::signal_update()
{
  DBUG_ENTER("MYSQL_BIN_LOG::signal_update");
  pthread_cond_broadcast(&update_cond);
  DBUG_VOID_RETURN;
}

void init_audit_log_tables(const char* comma_list)
{

  if (hash_init(&audit_log_tables, system_charset_info, 5,
                0, 0, (hash_get_key) audit_log_tables_get_key,
                0, 0))
  {
    sql_print_error("Initializing audit log tables failed.");
    unireg_abort(1);
  }
  is_audit_hashtable_inited= true;

  if (comma_list == NULL || strlen(comma_list) == 0)
    return;

  if (!strcasecmp(comma_list, "alltables"))
  {
    audit_log_all_tables= true;
  }
  else
  {
    audit_log_all_tables= false;
    audit_log_table_list= strdup(comma_list);
    char *p= audit_log_table_list;
    char *q;
    while ((q= strchr(p, ',')))
    {
      *q= 0;
      my_hash_insert(&audit_log_tables, (uchar *) p);
      p= q + 1;
    }
    if (strlen(p) > 0)
    {
      my_hash_insert(&audit_log_tables, (uchar *) p);
    }
  }
}

/**
  Determine whether or not a statement should be logged.

  @param[in]  thd       the connection object for this query
  @param[in]  command   the command

  @return  whether or not 'command' should be logged.
    @retval true   NOT logged
    @retval false  logged
*/

static bool audit_log_block_stmt(THD *thd, enum enum_sql_command command)
{
  if (command < 0 || command >= SQLCOM_END)
    return true;
  return audit_stmt_filter[command];
}

/**
   Adds a comma separator when the string is not empty.

   @param[in,out]  str   string to which we append a comma

   @return Status of the append operation
     @retval TRUE    append fails (out of memory)
     @retval FALSE   OK
*/

static bool separate_with_comma(String &str)
{
  if (!str.is_empty())
  {
    return str.append(',');
  }
  else
  {
    return false;
  }
}

/**
   Finds the first table in lex.select_lex which returns true.

   Traverses the list of of lex.select_lex tables calling visit_table for
   each one that has a table_name. Stops a traversal and returns true
   as soon as the first call to visit_table returns true. Returns false
   if evaluation went all the way to the end (that is it computes a
   boolean OR of all visit calls with short-circuiting).

   @param[in]  table   the list of tables to check

   @return whether visit_table is true for any of our tables
     @retval true   we have successfully visited a table
     @retval false  none of the tables were visited successfully.
*/

class TableVisitingAlgorithm
{
public:
  virtual bool visit_table(const TABLE_LIST& table)= 0;
  bool visit(THD* thd)
  {
    for (TABLE_LIST *table= (TABLE_LIST*) thd->lex->select_lex.table_list.first;
         table;
         table= table->next_global)
    {
      if (NULL != table->table_name)
      {
        /*
          add_table_to_list may guarantee table_name != NULL, but I
          can't tell easily.
        */
        if (visit_table(*table))
        {
          return true;
        }
      }
    }
    return false;
  }
};

/**
   Traverses the list of columns in #lex collecting all column names into #out.

   Item::print does not expose OOM conditions, a silent failure is
   possible. With luck, next separate_with_comma will notice. At least
   we are fortunate enough that memory corruption won't happen as
   String class just starts short-circuiting append calls when it
   fails to allocate memory.

   @param[in]      lex   the LEX object containing column data
   @param[in,out]  out   the string to which we are printing our
                         column data

   @return  Operation status
     @retval  true   memory allocation failure
     @retval  false  OK
*/

static bool compose_column_list(LEX* lex, String& out)
{
  List_iterator<Item> li(lex->select_lex.item_list);
  Item *pos;
  while ((pos=li++))
  {
    if (separate_with_comma(out))
    {
      return true;
    }
    pos->print(&out, QT_ORDINARY);
  }
  return false;
}

class AuditComputer : public TableVisitingAlgorithm
{
public:
  /**
     Check if #table is part of our audit list.

     @param[in]  table   the table to check

     @return Operation status
       @retval  true   #table is part of our audit list
       @retval  false  #table is not part of our audit list
  */
  bool visit_table(const TABLE_LIST& table)
  {
    return hash_search(&audit_log_tables, (uchar *) table.table_name,
                       strlen(table.table_name));
  }
};

class AliasCollector : public TableVisitingAlgorithm
{
public:
  String aliases_;
  /**
     Collect all affected tables along with their aliases (if any)
     into a string.

     #aliases_ will contain the current collection of table aliases as a string.

     @param[in]  table   the table to add to the collection

     @return Operation status
       @retval  true   out of memory ERROR
       @retval  false  OK
  */

  bool visit_table(const TABLE_LIST& table)
  {
    if (separate_with_comma(aliases_) ||
        aliases_.append(table.table_name))
    {
      return true;
    }
    if (table.alias != NULL && strcmp(table.alias, table.table_name))
    {
      if (aliases_.append(' ') ||
          aliases_.append(table.alias))
      {
        return true;
      }
    }
    return false;
  }
};

static const char* null_safe(const char *data)
{
  return data ? data : "null";
}


/**
  Determine whether a statement executed by a SUPER user should be logged.

  @param[in]  command   the command

  @return  whether or not 'command' should be logged.
    @retval true   NOT logged
    @retval false  logged
*/

static bool is_super_loggable(enum enum_sql_command command)
{
  return (is_update_query(command) ||
          command == SQLCOM_GRANT ||
          command == SQLCOM_CREATE_DB ||
          command == SQLCOM_ALTER_DB ||
          command == SQLCOM_REPAIR ||
          command == SQLCOM_REVOKE ||
          command == SQLCOM_REVOKE_ALL ||
          command == SQLCOM_OPTIMIZE ||
          command == SQLCOM_CHECK ||
          command == SQLCOM_ANALYZE ||
          command == SQLCOM_RENAME_TABLE ||
          command == SQLCOM_PURGE ||
          command == SQLCOM_DROP_USER ||
          command == SQLCOM_CREATE_USER ||
          command == SQLCOM_RENAME_USER ||
          command == SQLCOM_EXECUTE);
}


/**
   Write query information to the audit log.

   @param[in]  lex   the lex object for this query
   @param[in]  thd   the thread object for this connection
*/
void write_audit_record(LEX *lex, THD *thd)
{
  if (thd->main_security_ctx.user == NULL)
  {
    /*
      thd->user is NULL on the slave doing replication, in such a
      case the audit record has already been created on the master.
    */
    return;
  }

  /* For SUPER logging, log the full SQL. */
  if (opt_audit_log && opt_audit_log_super &&
      (thd->security_ctx->master_access & SUPER_ACL) &&
      is_super_loggable(lex->sql_command))
  {
    audit_log_print(thd, COM_QUERY, "%s@%s (%d): SUPER %s",
                    null_safe(thd->main_security_ctx.user),
                    null_safe(thd->main_security_ctx.host_or_ip),
                    lex->sql_command,
                    thd->query());
  }

  if (!(opt_audit_log && hash_inited(&audit_log_tables)))
  {
    return;
  }

  /*
    Check whether this particular statement should be logged,
    according to anything the user might have put on --audit_log_filter.
  */
  if (audit_log_block_stmt(thd, lex->sql_command))
    return;

  if (!audit_log_all_tables)
  {
    AuditComputer audit_computer;
    if (!audit_computer.visit(thd))
    {
      /*
        AuditComputer returned false, which means none of the tables in
        the query are monitored, hence there is nothing to audit.
      */
      return;
    }
  }

  AliasCollector alias_collector;
  if (alias_collector.visit(thd))
  {
    /* Aborts on an out of memory condition. */
    return;
  }

  String columns;
  char *detail= NULL;
  int count= -1;
  /*
    All switch cases populate detail with information about the query
    and must set count to a value != -1 if they succeed. If so, detail
    contains specific information about the query and must be freed
    before leaving this function.
  */
  switch (lex->sql_command)
  {
  case SQLCOM_SELECT:
    if (compose_column_list(lex, columns))
    {
      break;
    }
    count= asprintf(&detail,
                    "SELECT [%s] FROM [%s] [Sent %lu Rows]",
                    columns.c_ptr_safe(),
                    alias_collector.aliases_.c_ptr_safe(),
                    (unsigned long) thd->sent_row_count);
    break;
  case SQLCOM_UPDATE:
    if (compose_column_list(lex, columns))
    {
      break;
    }
    /*
       TODO: Fix these and following with row counts after stats functionality
       has been ported.
    */
    count= asprintf(&detail,
                    "UPDATE [%s] SET [%s]",
                    alias_collector.aliases_.c_ptr_safe(),
                    columns.c_ptr_safe());
    break;
  case SQLCOM_INSERT:
  case SQLCOM_INSERT_SELECT:
  case SQLCOM_REPLACE:
  case SQLCOM_REPLACE_SELECT:
    count= asprintf(&detail,
                    "INSERT INTO [%s]",
                    alias_collector.aliases_.c_ptr_safe());
    break;
  case SQLCOM_DELETE:
    count= asprintf(&detail,
                    "DELETE FROM [%s]",
                    alias_collector.aliases_.c_ptr_safe());
    break;
  default:
    /*
      Logs the raw text of the query in case it is not one of the
      known ones above. This seems to be safe as there is no case
      other SQL text would contain sensitive information.
    */
    count= asprintf(&detail,
                    "%s",
                    thd->query());
    break;
  }
  if (count != -1)
  {
    /*
      Some of the null_safe's below are unnecessary, they are only to
      make this code safer. I never want the code to abort when a
      NULL is printed.
    */
    audit_log_print(thd, COM_QUERY, "%s@%s (%d): %s",
                    null_safe(thd->main_security_ctx.user),
                    null_safe(thd->main_security_ctx.host_or_ip),
                    lex->sql_command,
                    null_safe(detail));
    if (detail)
    {
      free(detail);
    }
  }
  else
  {
    audit_log_print(thd, COM_QUERY, "Unable to build detail");
  }
}

void init_audit_log_filter(const char *comma_list)
{
  if (comma_list == NULL || comma_list == '\0')
    return;
  char *buf= strdup(comma_list);
  char *p= buf;
  while (*p != '\0')
  {
    char *q= strchr(p, ',');
    if (q != NULL)
      *q= '\0';

    /*
      User supplied option of SQL statements that should not be
      logged.  By default all statements are logged.
    */
    if (!strcasecmp(p, "select"))
      audit_stmt_filter[SQLCOM_SELECT]= true;
    else if (!strcasecmp(p, "insert"))
    {
      audit_stmt_filter[SQLCOM_INSERT]= true;
      audit_stmt_filter[SQLCOM_INSERT_SELECT]= true;
      audit_stmt_filter[SQLCOM_REPLACE]= true;
      audit_stmt_filter[SQLCOM_REPLACE_SELECT]= true;
    }
    else if (!strcasecmp(p, "delete"))
    {
      audit_stmt_filter[SQLCOM_DELETE]= true;
      audit_stmt_filter[SQLCOM_DELETE_MULTI]= true;
    }
    else if (!strcasecmp(p, "update"))
      audit_stmt_filter[SQLCOM_UPDATE]= true;
    else
    {
      sql_print_error("Unrecognised option '%s' to --audit_log_filter", p);
    }

    // This means we've reached the last item in the list.
    if (q == NULL)
      break;
    p= q + 1;
  }
  free(buf);
}


/*
  Called by mysql_binlog_send to notify MYSQL_LOG that a new dump
  thread has become active.
*/

bool MYSQL_BIN_LOG::enter_dump_thread(THD *thd)
{
  const char *old_msg;
  bool error= false;

  pthread_mutex_lock(&LOCK_reset_logs);
  old_msg= thd->enter_cond(&COND_no_reset_thd, &LOCK_reset_logs,
                           "Waiting for all RESET MASTERs to complete.");

  /* Don't allow a dump thread to spin up until all resets are complete. */
  while (resetting_logs)
  {
    pthread_cond_wait(&COND_no_reset_thd, &LOCK_reset_logs);
    if (thd->killed)
    {
      error= true;
      goto err;
    }
  }

  ++dump_thd_count;

err:
  thd->exit_cond(old_msg);

  return error;
}


/*
  Called by mysql_binlog_send to notify MYSQL_LOG that a dump
  thread has ceased to be active. Must be called if enter_dump_thread
  was called and it returned success (i.e. 0).
*/

bool MYSQL_BIN_LOG::exit_dump_thread(THD *thd)
{
  bool error= false;

  pthread_mutex_lock(&LOCK_reset_logs);

  if (dump_thd_count <= 0)
  {
    sql_print_error("exit_dump_thread called when enter_dump_thread wasn't "
                    "or returned error.");
    DBUG_ASSERT(false);
    error= true;
  }
  else if (!(--dump_thd_count))
  {
    pthread_cond_broadcast(&COND_no_dump_thd);
  }

  pthread_mutex_unlock(&LOCK_reset_logs);

  return error;
}


/*
  Called by callers of mysql_bin_log.reset_logs() prior to the reset_logs
  call. Ensures that all Binlog Dump threads have exited and that only
  a single thread can be 'prepared' to reset at a time.

  Not needed before calls to reset_logs which operate on the relay logs,
  only the bin log.
*/

bool MYSQL_BIN_LOG::prepare_for_reset_logs(THD *thd)
{
  const char *old_msg;
  bool error= false;

  /*
    LOCK_log can't be owned. If a Binlog Dump thread is currently in
    wait_for_update it would lead to a deadlock when kill_all_dump_threads
    calls awake() on it.
  */
  safe_mutex_assert_not_owner(&LOCK_log);

  /*
    LOCK_index can't be owned because it could lead to taking
    LOCK_log and LOCK_index in the opposite order from all other callers.
  */
  safe_mutex_assert_not_owner(&LOCK_index);

  pthread_mutex_lock(&LOCK_reset_logs);
  old_msg= thd->enter_cond(&COND_no_reset_thd, &LOCK_reset_logs,
                           "Waiting for all RESET MASTERs to complete.");

  /* Don't allow more than one reset_logs at a time. */
  while (resetting_logs)
  {
    pthread_cond_wait(&COND_no_reset_thd, &LOCK_reset_logs);
    if (thd->killed)
    {
      error= true;
      goto err2;
    }
  }
  resetting_logs= true;

  thd->proc_info= "Waiting for all Binlog Dump threads to exit.";
  thd->mysys_var->current_cond= &COND_no_dump_thd;

  /* Wait for all dump threads to exit. */
  while (dump_thd_count)
  {
    timeval now;
    timespec timeout;
    int retcode;

    /* Give them a helping hand toward exiting. */
    kill_all_dump_threads();

    /*
      The dump threads may miss the wake up if they weren't yet in
      wait_for_update and they don't check thd->killed before getting
      there. So, wait for 1 sec and if threads haven't exited, kick
      them again.
    */
    if (gettimeofday(&now, NULL))
    {
      error= true;
      goto err1;
    }
    timeout.tv_sec= now.tv_sec + 1;
    timeout.tv_nsec= now.tv_usec * 1000;
    retcode= pthread_cond_timedwait(&COND_no_dump_thd, &LOCK_reset_logs,
                                    &timeout);
    if (thd->killed || (retcode && retcode != ETIMEDOUT))
    {
      error= true;
      goto err1;
    }
  }

  thd->exit_cond(old_msg);

  return false;

err1:
  resetting_logs= false;
  pthread_cond_broadcast(&COND_no_reset_thd);
err2:
  thd->exit_cond(old_msg);

  return error;
}


/*
  Called by callers of mysql_bin_log.reset_logs() after the reset_logs
  call. Must be called if prepare_for_reset_logs was called and it
  returned success (i.e. 0).
*/
bool MYSQL_BIN_LOG::complete_reset_logs(THD *thd)
{
  bool error= false;

  pthread_mutex_lock(&LOCK_reset_logs);

  if (!resetting_logs)
  {
    sql_print_error("complete_reset_logs called when prepare_for_reset_logs "
                    "wasn't or returned error.");
    DBUG_ASSERT(false);
    error= true;
  }
  else
  {
    resetting_logs= false;
    pthread_cond_broadcast(&COND_no_reset_thd);
  }

  pthread_mutex_unlock(&LOCK_reset_logs);

  return error;
}


#ifdef __NT__
static void print_buffer_to_nt_eventlog(enum loglevel level, char *buff,
                                        size_t length, size_t buffLen)
{
  HANDLE event;
  char   *buffptr= buff;
  DBUG_ENTER("print_buffer_to_nt_eventlog");

  /* Add ending CR/LF's to string, overwrite last chars if necessary */
  strmov(buffptr+min(length, buffLen-5), "\r\n\r\n");

  setup_windows_event_source();
  if ((event= RegisterEventSource(NULL,"MySQL")))
  {
    switch (level) {
      case ERROR_LEVEL:
        ReportEvent(event, EVENTLOG_ERROR_TYPE, 0, MSG_DEFAULT, NULL, 1, 0,
                    (LPCSTR*)&buffptr, NULL);
        break;
      case WARNING_LEVEL:
        ReportEvent(event, EVENTLOG_WARNING_TYPE, 0, MSG_DEFAULT, NULL, 1, 0,
                    (LPCSTR*) &buffptr, NULL);
        break;
      case INFORMATION_LEVEL:
        ReportEvent(event, EVENTLOG_INFORMATION_TYPE, 0, MSG_DEFAULT, NULL, 1,
                    0, (LPCSTR*) &buffptr, NULL);
        break;
    }
    DeregisterEventSource(event);
  }

  DBUG_VOID_RETURN;
}
#endif /* __NT__ */


#ifndef EMBEDDED_LIBRARY
static void print_buffer_to_file(enum loglevel level, const char *buffer,
                                 size_t length)
{
  time_t skr;
  struct tm tm_tmp;
  struct tm *start;
  DBUG_ENTER("print_buffer_to_file");
  DBUG_PRINT("enter",("buffer: %s", buffer));

  VOID(pthread_mutex_lock(&LOCK_error_log));

  skr= my_time(0);
  localtime_r(&skr, &tm_tmp);
  start=&tm_tmp;

  fprintf(stderr, "%02d%02d%02d %2d:%02d:%02d [%s] %.*s\n",
          start->tm_year % 100,
          start->tm_mon+1,
          start->tm_mday,
          start->tm_hour,
          start->tm_min,
          start->tm_sec,
          (level == ERROR_LEVEL ? "ERROR" : level == WARNING_LEVEL ?
           "Warning" : "Note"),
          (int) length, buffer);

  fflush(stderr);

  VOID(pthread_mutex_unlock(&LOCK_error_log));
  DBUG_VOID_RETURN;
}

/**
  Prints a printf style message to the error log and, under NT, to the
  Windows event log.

  This function prints the message into a buffer and then sends that buffer
  to other functions to write that message to other logging sources.

  @param level          The level of the msg significance
  @param format         Printf style format of message
  @param args           va_list list of arguments for the message

  @returns
    The function always returns 0. The return value is present in the
    signature to be compatible with other logging routines, which could
    return an error (e.g. logging to the log tables)
*/
int vprint_msg_to_log(enum loglevel level, const char *format, va_list args)
{
  char   buff[1024];
  size_t length;
  DBUG_ENTER("vprint_msg_to_log");

  length= my_vsnprintf(buff, sizeof(buff), format, args);
  print_buffer_to_file(level, buff, length);

#ifdef __NT__
  print_buffer_to_nt_eventlog(level, buff, length, sizeof(buff));
#endif

  DBUG_RETURN(0);
}
#endif /* EMBEDDED_LIBRARY */


void sql_print_error(const char *format, ...) 
{
  va_list args;
  DBUG_ENTER("sql_print_error");

  va_start(args, format);
  error_log_print(ERROR_LEVEL, format, args);
  va_end(args);

  DBUG_VOID_RETURN;
}


void sql_print_warning(const char *format, ...) 
{
  va_list args;
  DBUG_ENTER("sql_print_warning");

  va_start(args, format);
  error_log_print(WARNING_LEVEL, format, args);
  va_end(args);

  DBUG_VOID_RETURN;
}


void sql_print_information(const char *format, ...) 
{
  va_list args;
  DBUG_ENTER("sql_print_information");

  va_start(args, format);
  error_log_print(INFORMATION_LEVEL, format, args);
  va_end(args);

  DBUG_VOID_RETURN;
}


/********* transaction coordinator log for 2pc - mmap() based solution *******/

/*
  the log consists of a file, mmapped to a memory.
  file is divided on pages of tc_log_page_size size.
  (usable size of the first page is smaller because of log header)
  there's PAGE control structure for each page
  each page (or rather PAGE control structure) can be in one of three
  states - active, syncing, pool.
  there could be only one page in active or syncing states,
  but many in pool - pool is fifo queue.
  usual lifecycle of a page is pool->active->syncing->pool
  "active" page - is a page where new xid's are logged.
  the page stays active as long as syncing slot is taken.
  "syncing" page is being synced to disk. no new xid can be added to it.
  when the sync is done the page is moved to a pool and an active page
  becomes "syncing".

  the result of such an architecture is a natural "commit grouping" -
  If commits are coming faster than the system can sync, they do not
  stall. Instead, all commit that came since the last sync are
  logged to the same page, and they all are synced with the next -
  one - sync. Thus, thought individual commits are delayed, throughput
  is not decreasing.

  when a xid is added to an active page, the thread of this xid waits
  for a page's condition until the page is synced. when syncing slot
  becomes vacant one of these waiters is awaken to take care of syncing.
  it syncs the page and signals all waiters that the page is synced.
  PAGE::waiters is used to count these waiters, and a page may never
  become active again until waiters==0 (that is all waiters from the
  previous sync have noticed the sync was completed)

  note, that the page becomes "dirty" and has to be synced only when a
  new xid is added into it. Removing a xid from a page does not make it
  dirty - we don't sync removals to disk.
*/

ulong tc_log_page_waits= 0;

#ifdef HAVE_MMAP

#define TC_LOG_HEADER_SIZE (sizeof(tc_log_magic)+1)

static const char tc_log_magic[]={(char) 254, 0x23, 0x05, 0x74};

ulong opt_tc_log_size= TC_LOG_MIN_SIZE;
ulong tc_log_max_pages_used=0, tc_log_page_size=0, tc_log_cur_pages_used=0;

int TC_LOG_MMAP::open(const char *opt_name)
{
  uint i;
  bool crashed=FALSE;
  PAGE *pg;

  DBUG_ASSERT(total_ha_2pc > 1);
  DBUG_ASSERT(opt_name && opt_name[0]);

  tc_log_page_size= my_getpagesize();
  DBUG_ASSERT(TC_LOG_PAGE_SIZE % tc_log_page_size == 0);

  fn_format(logname,opt_name,mysql_data_home,"",MY_UNPACK_FILENAME);
  if ((fd= my_open(logname, O_RDWR, MYF(0))) < 0)
  {
    if (my_errno != ENOENT)
      goto err;
    if (using_heuristic_recover())
      return 1;
    if ((fd= my_create(logname, CREATE_MODE, O_RDWR, MYF(MY_WME))) < 0)
      goto err;
    inited=1;
    file_length= opt_tc_log_size;
    if (my_chsize(fd, file_length, 0, MYF(MY_WME)))
      goto err;
  }
  else
  {
    inited= 1;
    crashed= TRUE;
    sql_print_information("Recovering after a crash using %s", opt_name);
    if (tc_heuristic_recover)
    {
      sql_print_error("Cannot perform automatic crash recovery when "
                      "--tc-heuristic-recover is used");
      goto err;
    }
    file_length= my_seek(fd, 0L, MY_SEEK_END, MYF(MY_WME+MY_FAE));
    if (file_length == MY_FILEPOS_ERROR || file_length % tc_log_page_size)
      goto err;
  }

  data= (uchar *)my_mmap(0, (size_t)file_length, PROT_READ|PROT_WRITE,
                        MAP_NOSYNC|MAP_SHARED, fd, 0);
  if (data == MAP_FAILED)
  {
    my_errno=errno;
    goto err;
  }
  inited=2;

  npages=(uint)file_length/tc_log_page_size;
  DBUG_ASSERT(npages >= 3);             // to guarantee non-empty pool
  if (!(pages=(PAGE *)my_malloc(npages*sizeof(PAGE), MYF(MY_WME|MY_ZEROFILL))))
    goto err;
  inited=3;
  for (pg=pages, i=0; i < npages; i++, pg++)
  {
    pg->next=pg+1;
    pg->waiters=0;
    pg->state=POOL;
    pthread_mutex_init(&pg->lock, MY_MUTEX_INIT_FAST);
    pthread_cond_init (&pg->cond, 0);
    pg->start=(my_xid *)(data + i*tc_log_page_size);
    pg->end=(my_xid *)(pg->start + tc_log_page_size);
    pg->size=pg->free=tc_log_page_size/sizeof(my_xid);
  }
  pages[0].size=pages[0].free=
                (tc_log_page_size-TC_LOG_HEADER_SIZE)/sizeof(my_xid);
  pages[0].start=pages[0].end-pages[0].size;
  pages[npages-1].next=0;
  inited=4;

  if (crashed && recover())
      goto err;

  memcpy(data, tc_log_magic, sizeof(tc_log_magic));
  data[sizeof(tc_log_magic)]= (uchar)total_ha_2pc;
  my_msync(fd, data, tc_log_page_size, MS_SYNC);
  inited=5;

  pthread_mutex_init(&LOCK_sync,    MY_MUTEX_INIT_FAST);
  pthread_mutex_init(&LOCK_active,  MY_MUTEX_INIT_FAST);
  pthread_mutex_init(&LOCK_pool,    MY_MUTEX_INIT_FAST);
  pthread_cond_init(&COND_active, 0);
  pthread_cond_init(&COND_pool, 0);

  inited=6;

  syncing= 0;
  active=pages;
  pool=pages+1;
  pool_last=pages+npages-1;

  return 0;

err:
  close();
  return 1;
}

/**
  there is no active page, let's got one from the pool.

  Two strategies here:
    -# take the first from the pool
    -# if there're waiters - take the one with the most free space.

  @todo
    TODO page merging. try to allocate adjacent page first,
    so that they can be flushed both in one sync
*/

void TC_LOG_MMAP::get_active_from_pool()
{
  PAGE **p, **best_p=0;
  int best_free;

  if (syncing)
    pthread_mutex_lock(&LOCK_pool);

  do
  {
    best_p= p= &pool;
    if ((*p)->waiters == 0) // can the first page be used ?
      break;                // yes - take it.

    best_free=0;            // no - trying second strategy
    for (p=&(*p)->next; *p; p=&(*p)->next)
    {
      if ((*p)->waiters == 0 && (*p)->free > best_free)
      {
        best_free=(*p)->free;
        best_p=p;
      }
    }
  }
  while ((*best_p == 0 || best_free == 0) && overflow());

  active=*best_p;
  if (active->free == active->size) // we've chosen an empty page
  {
    tc_log_cur_pages_used++;
    set_if_bigger(tc_log_max_pages_used, tc_log_cur_pages_used);
  }

  if ((*best_p)->next)              // unlink the page from the pool
    *best_p=(*best_p)->next;
  else
    pool_last=*best_p;

  if (syncing)
    pthread_mutex_unlock(&LOCK_pool);
}

/**
  @todo
  perhaps, increase log size ?
*/
int TC_LOG_MMAP::overflow()
{
  /*
    simple overflow handling - just wait
    TODO perhaps, increase log size ?
    let's check the behaviour of tc_log_page_waits first
  */
  tc_log_page_waits++;
  pthread_cond_wait(&COND_pool, &LOCK_pool);
  return 1; // always return 1
}

/**
  Record that transaction XID is committed on the persistent storage.

    This function is called in the middle of two-phase commit:
    First all resources prepare the transaction, then tc_log->log() is called,
    then all resources commit the transaction, then tc_log->unlog() is called.

    All access to active page is serialized but it's not a problem, as
    we're assuming that fsync() will be a main bottleneck.
    That is, parallelizing writes to log pages we'll decrease number of
    threads waiting for a page, but then all these threads will be waiting
    for a fsync() anyway

   If tc_log == MYSQL_LOG then tc_log writes transaction to binlog and
   records XID in a special Xid_log_event.
   If tc_log = TC_LOG_MMAP then xid is written in a special memory-mapped
   log.

  @retval
    0  - error
  @retval
    \# - otherwise, "cookie", a number that will be passed as an argument
    to unlog() call. tc_log can define it any way it wants,
    and use for whatever purposes. TC_LOG_MMAP sets it
    to the position in memory where xid was logged to.
*/

int TC_LOG_MMAP::log_xid(THD *thd, my_xid xid)
{
  int err;
  PAGE *p;
  ulong cookie;

  pthread_mutex_lock(&LOCK_active);

  /*
    if active page is full - just wait...
    frankly speaking, active->free here accessed outside of mutex
    protection, but it's safe, because it only means we may miss an
    unlog() for the active page, and we're not waiting for it here -
    unlog() does not signal COND_active.
  */
  while (unlikely(active && active->free == 0))
    pthread_cond_wait(&COND_active, &LOCK_active);

  /* no active page ? take one from the pool */
  if (active == 0)
    get_active_from_pool();

  p=active;
  pthread_mutex_lock(&p->lock);

  /* searching for an empty slot */
  while (*p->ptr)
  {
    p->ptr++;
    DBUG_ASSERT(p->ptr < p->end);               // because p->free > 0
  }

  /* found! store xid there and mark the page dirty */
  cookie= (ulong)((uchar *)p->ptr - data);      // can never be zero
  *p->ptr++= xid;
  p->free--;
  p->state= DIRTY;

  /* to sync or not to sync - this is the question */
  pthread_mutex_unlock(&LOCK_active);
  pthread_mutex_lock(&LOCK_sync);
  pthread_mutex_unlock(&p->lock);

  if (syncing)
  {                                          // somebody's syncing. let's wait
    p->waiters++;
    /*
      note - it must be while (), not do ... while () here
      as p->state may be not DIRTY when we come here
    */
    while (p->state == DIRTY && syncing)
      pthread_cond_wait(&p->cond, &LOCK_sync);
    p->waiters--;
    err= p->state == ERROR;
    if (p->state != DIRTY)                   // page was synced
    {
      if (p->waiters == 0)
        pthread_cond_signal(&COND_pool);     // in case somebody's waiting
      pthread_mutex_unlock(&LOCK_sync);
      goto done;                             // we're done
    }
  }                                          // page was not synced! do it now
  DBUG_ASSERT(active == p && syncing == 0);
  pthread_mutex_lock(&LOCK_active);
  syncing=p;                                 // place is vacant - take it
  active=0;                                  // page is not active anymore
  pthread_cond_broadcast(&COND_active);      // in case somebody's waiting
  pthread_mutex_unlock(&LOCK_active);
  pthread_mutex_unlock(&LOCK_sync);
  err= sync();

done:
  return err ? 0 : cookie;
}

int TC_LOG_MMAP::sync()
{
  int err;

  DBUG_ASSERT(syncing != active);

  /*
    sit down and relax - this can take a while...
    note - no locks are held at this point
  */
  err= my_msync(fd, syncing->start, 1, MS_SYNC);

  /* page is synced. let's move it to the pool */
  pthread_mutex_lock(&LOCK_pool);
  pool_last->next=syncing;
  pool_last=syncing;
  syncing->next=0;
  syncing->state= err ? ERROR : POOL;
  pthread_cond_broadcast(&syncing->cond);    // signal "sync done"
  pthread_cond_signal(&COND_pool);           // in case somebody's waiting
  pthread_mutex_unlock(&LOCK_pool);

  /* marking 'syncing' slot free */
  pthread_mutex_lock(&LOCK_sync);
  syncing=0;
  pthread_cond_signal(&active->cond);        // wake up a new syncer
  pthread_mutex_unlock(&LOCK_sync);
  return err;
}

/**
  erase xid from the page, update page free space counters/pointers.
  cookie points directly to the memory where xid was logged.
*/

int TC_LOG_MMAP::unlog(ulong cookie, my_xid xid)
{
  PAGE *p=pages+(cookie/tc_log_page_size);
  my_xid *x=(my_xid *)(data+cookie);

  DBUG_ASSERT(*x == xid);
  DBUG_ASSERT(x >= p->start && x < p->end);
  *x=0;

  pthread_mutex_lock(&p->lock);
  p->free++;
  DBUG_ASSERT(p->free <= p->size);
  set_if_smaller(p->ptr, x);
  if (p->free == p->size)               // the page is completely empty
    statistic_decrement(tc_log_cur_pages_used, &LOCK_status);
  if (p->waiters == 0)                 // the page is in pool and ready to rock
    pthread_cond_signal(&COND_pool);   // ping ... for overflow()
  pthread_mutex_unlock(&p->lock);
  return 0;
}

void TC_LOG_MMAP::close()
{
  uint i;
  switch (inited) {
  case 6:
    pthread_mutex_destroy(&LOCK_sync);
    pthread_mutex_destroy(&LOCK_active);
    pthread_mutex_destroy(&LOCK_pool);
    pthread_cond_destroy(&COND_pool);
  case 5:
    data[0]='A'; // garble the first (signature) byte, in case my_delete fails
  case 4:
    for (i=0; i < npages; i++)
    {
      if (pages[i].ptr == 0)
        break;
      pthread_mutex_destroy(&pages[i].lock);
      pthread_cond_destroy(&pages[i].cond);
    }
  case 3:
    my_free((uchar*)pages, MYF(0));
  case 2:
    my_munmap((char*)data, (size_t)file_length);
  case 1:
    my_close(fd, MYF(0));
  }
  if (inited>=5) // cannot do in the switch because of Windows
    my_delete(logname, MYF(MY_WME));
  inited=0;
}

int TC_LOG_MMAP::recover()
{
  HASH xids;
  PAGE *p=pages, *end_p=pages+npages;

  if (memcmp(data, tc_log_magic, sizeof(tc_log_magic)))
  {
    sql_print_error("Bad magic header in tc log");
    goto err1;
  }

  /*
    the first byte after magic signature is set to current
    number of storage engines on startup
  */
  if (data[sizeof(tc_log_magic)] != total_ha_2pc)
  {
    sql_print_error("Recovery failed! You must enable "
                    "exactly %d storage engines that support "
                    "two-phase commit protocol",
                    data[sizeof(tc_log_magic)]);
    goto err1;
  }

  if (hash_init(&xids, &my_charset_bin, tc_log_page_size/3, 0,
                sizeof(my_xid), 0, 0, MYF(0)))
    goto err1;

  for ( ; p < end_p ; p++)
  {
    for (my_xid *x=p->start; x < p->end; x++)
      if (*x && my_hash_insert(&xids, (uchar *)x))
        goto err2; // OOM
  }

  if (ha_recover(&xids))
    goto err2;

  hash_free(&xids);
  bzero(data, (size_t)file_length);
  return 0;

err2:
  hash_free(&xids);
err1:
  sql_print_error("Crash recovery failed. Either correct the problem "
                  "(if it's, for example, out of memory error) and restart, "
                  "or delete tc log and start mysqld with "
                  "--tc-heuristic-recover={commit|rollback}");
  return 1;
}
#endif

TC_LOG *tc_log;
TC_LOG_DUMMY tc_log_dummy;
TC_LOG_MMAP  tc_log_mmap;

/**
  Perform heuristic recovery, if --tc-heuristic-recover was used.

  @note
    no matter whether heuristic recovery was successful or not
    mysqld must exit. So, return value is the same in both cases.

  @retval
    0	no heuristic recovery was requested
  @retval
    1   heuristic recovery was performed
*/

int TC_LOG::using_heuristic_recover()
{
  if (!tc_heuristic_recover)
    return 0;

  sql_print_information("Heuristic crash recovery mode");
  if (ha_recover(0))
    sql_print_error("Heuristic crash recovery failed");
  sql_print_information("Please restart mysqld without --tc-heuristic-recover");
  return 1;
}

/****** transaction coordinator log for 2pc - binlog() based solution ******/
#define TC_LOG_BINLOG MYSQL_BIN_LOG

/**
  @todo
  keep in-memory list of prepared transactions
  (add to list in log(), remove on unlog())
  and copy it to the new binlog if rotated
  but let's check the behaviour of tc_log_page_waits first!
*/

int TC_LOG_BINLOG::open(const char *opt_name)
{
  LOG_INFO log_info;
  int      error= 1;

  DBUG_ASSERT(group_id_only_crash_recovery || total_ha_2pc > 1);
  DBUG_ASSERT(opt_name && opt_name[0]);
  DBUG_ASSERT(rpl_hierarchical || !group_id_only_crash_recovery);

  pthread_mutex_init(&LOCK_prep_xids, MY_MUTEX_INIT_FAST);
  pthread_cond_init (&COND_prep_xids, 0);

  if (!my_b_inited(&index_file))
  {
    /* There was a failure to open the index file, can't open the binlog */
    cleanup();
    return 1;
  }

  if (!group_id_only_crash_recovery && using_heuristic_recover())
  {
    /* generate a new binlog to mask a corrupted one */
    open(opt_name, LOG_BIN, 0, WRITE_CACHE, 0, max_binlog_size, 0, TRUE);
    cleanup();
    return 1;
  }

  if ((error= find_log_pos(&log_info, NullS, 1)))
  {
    if (error != LOG_INFO_EOF)
      sql_print_error("find_log_pos() failed (error: %d)", error);
    else
      error= 0;
    goto err;
  }

  {
    const char *errmsg;
    IO_CACHE    log;
    File        file;
    Log_event  *ev=0;
    Format_description_log_event fdle(BINLOG_VERSION);
    char        log_name[FN_REFLEN];

    if (! fdle.is_valid())
      goto err;

    do
    {
      strmake(log_name, log_info.log_file_name, sizeof(log_name)-1);
      /*
        Update group_id to match the value we read out of the index
        file. When loop exits, it will be the value of the last line
        in the index. It may be overwritten later in recover, if the
        recovered bin log contains events with IDs larger than what
        was in the index.
      */
      if (rpl_hierarchical)
      {
        group_id= log_info.group_id;
        last_event_server_id= log_info.server_id;
      }
    } while (!(error= find_next_log(&log_info, 1)));

    if (error !=  LOG_INFO_EOF)
    {
      sql_print_error("find_log_pos() failed (error: %d)", error);
      goto err;
    }

    if (rpl_hierarchical)
    {
      char llbuf[22];
      snprintf(llbuf, 22, "%llu", group_id);
      sql_print_information("Read group_id=%s, server_id=%u from index file.",
                            llbuf, last_event_server_id);
    }

    if ((file= open_binlog(&log, log_name, &errmsg)) < 0)
    {
      sql_print_error("%s", errmsg);
      goto err;
    }

    if ((ev= Log_event::read_log_event(&log, 0, &fdle)) &&
        ev->get_type_code() == FORMAT_DESCRIPTION_EVENT &&
        ev->flags & LOG_EVENT_BINLOG_IN_USE_F)
    {
      sql_print_information("Recovering after a crash using %s", opt_name);
      error= recover(&log, (Format_description_log_event *)ev);

      /*
        Fix index to have the correct group_id.
      */
      if (rpl_hierarchical)
      {
        char llbuf[22];
        snprintf(llbuf, 22, "%llu", group_id);
        sql_print_information("Recovered group_id=%s, server_id=%u from "
                              "most recent binlog.",
                              llbuf, last_event_server_id);

        error|= write_group_id_to_index(log_name, true, true);

        /*
          If this server is a slave, and if a transaction was in the process
          of being committed when the server crashed, and the server was
          running with bin log enabled (which is why this function is
          running), it's possible that the state of the database is now
          inconsistent with relay-log.info and, if it was also running with
          rpl_transaction_enabled, could even be inconsistent with InnoDB's
          replication state. Specifically, if the server crashed after a
          transaction was written to the bin log, but before it was committed
          to InnoDB, that transaction was just committed as part of the
          recover call above, but neither relay-log.info nor InnoDB's
          replication status were updated. The above scenario, if nothing
          is done about it, would result in an attempt to replay a
          transaction when the replication SQL thread starts up, possibly
          resulting in data discrepencies between the slave and the master
          or replication stopping with an error (e.g. attempting to insert
          a duplicate entry for a key).

          Thus, if we're running with rpl_hierarchical, ignore relay-log.info
          after a crash recovery. Instead, reset the slave and turn on
          connect_using_group_id.

          NOTE: It's possible that this slave is running with
          rpl_hierarchical while the master is not. In that case turning on
          connect_using_group_id would cause the replication IO thread
          to get back an error from the master. A heuristing for telling
          if the master is not running with rpl_hierarchial is 'group_id == 0'
          (see get_group_id_to_use()).
        */
        if (rpl_hierarchical_slave_recovery && group_id)
          do_rpl_hierarchical_slave_recovery= true;
      }
    }
    else
      error=0;

    delete ev;
    end_io_cache(&log);
    my_close(file, MYF(MY_WME));

    if (error)
      goto err;
  }

err:
  return error;
}

/** This is called on shutdown, after ha_panic. */
void TC_LOG_BINLOG::close()
{
  DBUG_ASSERT(prepared_xids==0);
  pthread_mutex_destroy(&LOCK_prep_xids);
  pthread_cond_destroy (&COND_prep_xids);
}

/**
  @todo
  group commit

  @retval
    0    error
  @retval
    1    success
*/
int TC_LOG_BINLOG::log_xid(THD *thd, my_xid xid)
{
  DBUG_ENTER("TC_LOG_BINLOG::log");
  Xid_log_event xle(thd, xid);
  binlog_trx_data *trx_data=
    (binlog_trx_data*) thd_get_ha_data(thd, binlog_hton);
  /*
    We always commit the entire transaction when writing an XID. Also
    note that the return value is inverted.
   */
  DBUG_RETURN(!binlog_end_trans(thd, trx_data, &xle, TRUE));
}

int TC_LOG_BINLOG::unlog(ulong cookie, my_xid xid)
{
  DBUG_ENTER("TC_LOG_BINLOG::unlog");
  pthread_mutex_lock(&LOCK_prep_xids);
  DBUG_ASSERT(prepared_xids > 0);
  if (--prepared_xids == 0) {
    DBUG_PRINT("info", ("prepared_xids=%lu", prepared_xids));
    pthread_cond_signal(&COND_prep_xids);
  }
  pthread_mutex_unlock(&LOCK_prep_xids);
  DBUG_RETURN(rotate_and_purge(0));     // as ::write() did not rotate
}

int TC_LOG_BINLOG::recover(IO_CACHE *log, Format_description_log_event *fdle)
{
  Log_event  *ev;
  HASH xids;
  MEM_ROOT mem_root;
  bool in_trans= false;

  if (!fdle->is_valid())
    goto err1;
  if (!group_id_only_crash_recovery &&
      hash_init(&xids, &my_charset_bin, TC_LOG_PAGE_SIZE/3, 0,
                sizeof(my_xid), 0, 0, MYF(0)))
    goto err1;

  init_alloc_root(&mem_root, TC_LOG_PAGE_SIZE, TC_LOG_PAGE_SIZE);

  fdle->flags&= ~LOG_EVENT_BINLOG_IN_USE_F; // abort on the first error

  while ((ev= Log_event::read_log_event(log,0,fdle)) && ev->is_valid())
  {
    if (!group_id_only_crash_recovery && ev->get_type_code() == XID_EVENT)
    {
      Xid_log_event *xev=(Xid_log_event *)ev;
      uchar *x= (uchar *) memdup_root(&mem_root, (uchar*) &xev->xid,
                                      sizeof(xev->xid));
      if (!x || my_hash_insert(&xids, x))
        goto err2;
    }

    /*
      Index file is only updated with correct group_id on a clean shutdown.
      If we didn't shutdown cleanly, need to recover the group_id for the
      last group this server committed.
    */
    if (rpl_hierarchical &&
        ev->get_type_code() != ROTATE_EVENT &&
        ev->get_type_code() != FORMAT_DESCRIPTION_EVENT)
    {
      /*
        Look for BEGIN and COMMIT events so that we can keep track
        of when events are part of a transaction. Ignore all events
        which are part of a transaction except for COMMIT.
      */
      if (ev->get_type_code() == QUERY_EVENT)
      {
        Query_log_event *qev= (Query_log_event *) ev;

        DBUG_ASSERT(!strncmp(query_with_log[0].query_, "BEGIN", 6));
        DBUG_ASSERT(!strncmp(query_with_log[1].query_, "COMMIT", 7));
        for (int idx= 0; idx < 2; ++idx)
        {
          if (qev->q_len == query_with_log[idx].query_length_ &&
              !strncmp(qev->query, query_with_log[idx].query_, qev->q_len))
          {
            in_trans= !idx;
            break;
          }
        }
      }
      else if (ev->get_type_code() == XID_EVENT)
        in_trans= false;

      /*
        group_id may have been flushed to the index file as part of a
        SET BINLOG_GROUP_ID=<value>; statement prior to an unclean shutdown.
        Thus, we may loop over events with a group_id less than the value we
        previously read out of the index and so take the max of the values.
      */
      if (!in_trans && group_id < ev->group_id)
      {
        group_id= ev->group_id;
        last_event_server_id= ev->server_id;
      }
    }

    delete ev;
  }

  if (!group_id_only_crash_recovery && ha_recover(&xids))
    goto err2;

  free_root(&mem_root, MYF(0));
  if (!group_id_only_crash_recovery)
    hash_free(&xids);
  return 0;

err2:
  free_root(&mem_root, MYF(0));
  if (!group_id_only_crash_recovery)
    hash_free(&xids);
err1:
  sql_print_error("Crash recovery failed. Either correct the problem "
                  "(if it's, for example, out of memory error) and restart, "
                  "or delete (or rename) binary log and start mysqld with "
                  "--tc-heuristic-recover={commit|rollback}");
  return 1;
}

/**
  Updates server's notion of the current group_id.

  This function is called in response to a SET BINLOG_GROUP_ID command. It
  protects the group_id variable with LOCK_log and always rotates to a new
  log so that the index file gets updated with an entry which contains
  group_id_arg.

  @param  thd              the curren thd
  @param  group_id_arg     the new group_id
  @param  server_id_arg    the new server_id
  @param  do_reset_master  whether to reset the master as part of the command

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::update_group_id(THD *thd, ulonglong group_id_arg,
                                   uint32 server_id_arg, bool do_reset_master)
{
  ulonglong rpl_hier_cache_freq_save;

  if (!rpl_hierarchical)
  {
    my_error(ER_OPTION_PREVENTS_STATEMENT, MYF(0), "--skip-rpl-hierarchical");
    return 1;
  }

  if (do_reset_master && prepare_for_reset_logs(thd))
  {
    if (thd->killed)
      thd->send_kill_message();
    else
      my_message(ER_LOG_PURGE_UNKNOWN_ERR,
                 ER(ER_LOG_PURGE_UNKNOWN_ERR), MYF(0));
    return 1;
  }

  pthread_mutex_lock(&LOCK_log);

  /*
    Disallow setting the group_id to be smaller than what the server
    has already seen. Doing so causes the repl_hier_cache to explode and
    the non-cached version of get_log_info_for_group_id to error out.
  */
  if (group_id_arg < group_id && !do_reset_master)
  {
    my_error(ER_STATEMENT_REQUIRES_OPTION, MYF(0), "WITH RESET");
    pthread_mutex_unlock(&LOCK_log);
    return 1;
  }

  /*
    Update group_id before any reset so that the group_id value written
    to the index file is the new one.
  */
  group_id= group_id_arg;
  last_event_server_id= server_id_arg;

  if (do_reset_master)
  {
    /*
      Because we may have just changed group_id to be a lower value
      than what has been written to the logs/index/cache we have to
      jump through some hoops in order to ensure that the cache
      ends up in a valid state.
    */
    rpl_hier_cache_freq_save= rpl_hier_cache_frequency_real;
    rpl_hier_cache_frequency_real= 0;

    repl_hier_cache_clear();

    pthread_mutex_lock(&LOCK_index);
    (void) reset_logs(thd, false /* need_lock */);
    pthread_mutex_unlock(&LOCK_index);

    rpl_hier_cache_frequency_real= rpl_hier_cache_freq_save;

    /*
      Now that logs and index have the new group_id,
      initialize cache if it is enabled.
    */
    if (rpl_hier_cache_frequency_real)
      /* An error in initialize will just disable the cache. */
      initialize_repl_hier_cache(false);
  }

  /*
    The DB admin used 'SET BINLOG_GROUP_ID' to update group_id. However,
    because MYSQL_LOG::group_id holds the last consumed ID, the next events
    written to the bin log will have (group_id_arg + 1) and so no events
    which actually match group_id_arg will show up in the bin log. This
    causes problems if the server is receiving events and later the DB
    admin does a 'SHOW BINLOG INFO FOR' or the slave tries to connect
    using a group_id which matches the group_id_arg used here. The solution,
    rotate_and_purge here to start a new bin log, which has the side-effect
    of writing group_id_arg to the index file. Then, in
    get_log_info_for_group_id, special case looking for a group_id which
    exactly matches a value in the index file.
  */
  rotate_and_purge(RP_LOCK_LOG_IS_ALREADY_LOCKED | RP_FORCE_ROTATE);

  pthread_mutex_unlock(&LOCK_log);

  if (do_reset_master)
    (void) mysql_bin_log.complete_reset_logs(thd);

  return 0;
}

/**
  Helper function which opens a bin log file and verifies that the events
  in it contain group_ids.

  @param  log_file_name  name of the file to open
  @param  log_lock       if non-NULL, the lock to take when reading from
                         the file
  @param[out]  file      receives the File so that caller can close it
  @param[out]  log       receives the IO_CACHE caller can use to read the file

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::open_binlog_with_group_id(const char *log_file_name,
                                             pthread_mutex_t *log_lock,
                                             File *file, IO_CACHE *log)
{
  const char *errmsg;
  Log_event *ev= 0;
  Format_description_log_event fdle(BINLOG_VERSION);
  uint8 common_header_len;

  if (!fdle.is_valid())
  {
    sql_print_error("MYSQL_LOG::open_binlog_with_group_id failed at "
                    "!fdle.is_valid()");
    goto err1;
  }

  if ((*file= open_binlog(log, log_file_name, &errmsg)) < 0)
  {
    sql_print_error("MYSQL_LOG::open_binlog_with_group_id failed at "
                    "open_binlog with message: %s", errmsg);
    goto err1;
  }

  if (!(ev= Log_event::read_log_event(log, log_lock, &fdle)))
  {
    sql_print_error("MYSQL_LOG::open_binlog_with_group_id failed at "
                    "read_log_event");
    goto err2;
  }

  if (ev->get_type_code() != FORMAT_DESCRIPTION_EVENT)
  {
    sql_print_error("MYSQL_LOG::open_binlog_with_group_id failed at "
                    "first event is not FORMAT_DESCRIPTION_EVENT");
    goto err3;
  }

  common_header_len= ((Format_description_log_event *) ev)->common_header_len;
  if (common_header_len < LOG_EVENT_HEADER_WITH_ID_LEN)
  {
    sql_print_error("MYSQL_LOG::open_binlog_with_group_id failed at "
                    "common header not long enough to contain group_id");
    goto err3;
  }

  delete ev;
  return 0;

 err3:
  delete ev;
 err2:
  end_io_cache(log);
  my_close(*file, MYF(MY_WME));
 err1:
  return 1;
}

/**
  Initializes the hierarchical replication cache.

  This function is called at server initialization if cache is enabled and
  at runtime when the cache switches from disabled to enabled. It reads
  the index file and adds entries to the cache for each index entry.

  @param  need_lock  Set to 1 if caller has not locked LOCK_log

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::initialize_repl_hier_cache(bool need_lock)
{
  LOG_INFO linfo;
  int error;
  int ret_val= 0;

  DBUG_ASSERT(rpl_hierarchical);

  /* Cache is protected by bin log's LOCK_log. */
  if (need_lock)
    pthread_mutex_lock(&LOCK_log);
  safe_mutex_assert_owner(&LOCK_log);
  /* And we're reading the index so protect it. */
  pthread_mutex_lock(&LOCK_index);

  /*
    Get first bin log, if there is one. The case where there isn't is
    during init_server_components on a brand new system.
  */
  if (!(error= find_log_pos(&linfo, NullS, false)))
  {
    do
    {
      repl_hier_cache_append_file(linfo.group_id, linfo.server_id,
                                  linfo.log_file_name);
    } while (!(error= find_next_log(&linfo, false)));
  }

  if (error !=  LOG_INFO_EOF)
  {
    sql_print_error("MYSQL_LOG::initialize_repl_hier_cache failed parsing "
                    "index with error = %d. Diabling the cache.", error);
    repl_hier_cache_disable();
    ret_val= 1;
  }

  if (need_lock)
    pthread_mutex_unlock(&LOCK_log);
  pthread_mutex_unlock(&LOCK_index);

  return ret_val;
}

/**
  Retrieve a LOG_INFO for the specified group_id from the cache.

  Attempts to retrieve a LOG_INFO from the hierarchical replication cache.
  First queries the cache for a starting log and position. Then does a
  sequential scan of the file for the matching group_id. Acquires LOCK_log
  to protect rpl_hier_cache_frequency_real. Because the lock needs to be
  taken to determine whether cache is enabled, the check is done here and
  results returned to caller via used_cache.

  @param       thd           the current thd
  @param       group_id_arg  the group_id to lookup
  @param[out]  linfo         receives the results of the lookup
  @param[out]  used_cache    receives whether the cache was used. If not,
                             caller should do an uncached lookup.

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::cached_get_log_info_for_group_id(THD *thd,
                                                    ulonglong group_id_arg,
                                                    LOG_INFO *linfo,
                                                    bool *used_cache)
{
  int ret_val= 1;
  bool need_unlock= true;
  pthread_mutex_t *log_lock= NULL;
  const char *log_file;
  bool returned_log_file_boundary= false;

  pthread_mutex_lock(&LOCK_log);
  if (rpl_hier_cache_frequency_real)
  {
    /* Cache is on so we used it regardless of success return. */
    *used_cache= true;

    /*
      Query the cache to see if we get a hit. Note that a hit does not
      mean a matching group_id. Rather, a hit means that we get a starting
      log_file_name and pos. The pos is important because it means we only
      have to sequentially scan a portion of the file rather than the whole
      thing.
    */
    if (!repl_hier_cache_lookup(group_id /* server's current ID */,
                                last_event_server_id, /* and current srv ID */
                                group_id_arg /* target ID */,
                                &linfo->group_id /* cache entry found ID */,
                                &linfo->server_id,
                                &linfo->pos, &log_file,
                                &returned_log_file_boundary))
    {
      /*
        If we're not in the special case of getting back a file boundary,
        the returned cache entry's ID must be less than or equal to the
        target ID.
      */
      DBUG_ASSERT(returned_log_file_boundary ||
                  linfo->group_id <= group_id_arg);

      strncpy(linfo->log_file_name, log_file, LOG_NAME_LEN);

      /*
        If the cache used a boundary, then it gave imcomplete information.
        Specificially, it didn't give a pos.
      */
      if (returned_log_file_boundary)
      {
        /*
          We do different things in this situation. If the target ID matches
          the log file boundary ID, we want to use the end of the log as the
          pos. Otherwise, we scan from the beginning of the log.
        */
        if (group_id_arg == linfo->group_id)
        {
          /*
            Special case to handle group_ids set via 'SET BINLOG_GROUP_ID'.
            So that we can find those group_ids even after new events have
            come through the system. This works because the implementation of
            update_group_id rotates to a new log when the group_id is set.
          */
          MY_STAT stat;
          if (!my_stat(linfo->log_file_name, &stat, MYF(0)))
          {
            sql_print_information("Failed to execute my_stat on file '%s'",
                                  linfo->log_file_name);
            goto err1;
          }

          linfo->pos= stat.st_size;
        }
        else
        {
          /*
            If it wasn't a match, means that we need to start the scan at the
            beginning of the log.
          */
          linfo->pos= BIN_LOG_HEADER_SIZE;
        }
      }

      /*
        Scan through the log if cache hit is before the target ID. Otherwise,
        we can just return what we got back from the cache.
      */
      if (linfo->group_id != group_id_arg)
      {
        File file;
        IO_CACHE log;

        /*
          We don't want to hold LOCK_log for a long time while we scan through
          the file. Check to see if the hit is for the active log. If so,
          only take the lock during reads. If not, no need to lock at all.
        */
        if (!strcmp(linfo->log_file_name, log_file_name))
          log_lock= &LOCK_log;

        need_unlock= false;
        pthread_mutex_unlock(&LOCK_log);

        if (!open_binlog_with_group_id(linfo->log_file_name, log_lock, &file,
                                       &log))
        {
          DBUG_ASSERT(linfo->pos <= my_b_filelength(&log));
          my_b_seek(&log, linfo->pos);

          ret_val= scan_bin_log_events(&log, linfo->log_file_name,
                                       INFO_OF_ID, log_lock,
                                       &group_id_arg, &linfo->pos,
                                       &linfo->server_id);
          end_io_cache(&log);
          my_close(file, MYF(MY_WME));
        }
      }
      else
        ret_val= 0;
    }
    /*
      No else clause here because no error message is printed out for failure
      to lookup in the cache because user may have simply asked for an ID the
      server doesn't have.
    */
  }

 err1:
  if (need_unlock)
    pthread_mutex_unlock(&LOCK_log);

  return ret_val;
}

/**
  Retrieve a LOG_INFO for the specified group_id without using the cache.

  First scans the index file looking for a log which should contain
  the requested group_id. If found, then scans the log for the matching
  group_id. If the current log has to be scanned, will acquire LOCK_log
  to read it safely.

  @param       thd           the current thd
  @param       group_id_arg  the group_id to lookup
  @param[out]  linfo         receives the results of the lookup

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::uncached_get_log_info_for_group_id(THD *thd,
                                                      ulonglong group_id_arg,
                                                      LOG_INFO *linfo)
{
  int ret_val= 1;
  pthread_mutex_t *log_lock= NULL;
  bool active_log= false;
  int error;

  DBUG_ASSERT(!thd->current_linfo);
  thd->current_linfo= linfo;

  /*
    Read through the index file looking for an entry with a group_id
    which is greater than then one for which we're looking.
  */
  if ((error= find_log_pos(linfo, NullS, true)))
  {
    /*
      EOF here is an unexpected error. We're running with --rpl_hierarchical
      which means that we're also running with --log-bin, which means that
      we should have a bin log and index file to process.
    */
    sql_print_error("MYSQL_LOG::get_log_info_for_group_id call to "
                    "find_log_pos() failed with error = %d)", error);
    goto err1;
  }

  while (linfo->group_id < group_id_arg)
  {
    if ((error= find_next_log(linfo, true)))
    {
      if (error !=  LOG_INFO_EOF)
      {
        sql_print_error("MYSQL_LOG::get_log_info_for_group_id call to "
                        "find_next_pos() failed with error = %d)", error);

        goto err1;
      }

      /*
        On LOG_INFO_EOF break out of the loop and search the current log for
        a matching ID. We need to do this because the index file isn't updated
        with the ID until the corresponding bin log is closed and so
        linfo->group_id isn't accurate for the currently open log.
      */
      if (get_current_log(linfo))
      {
        sql_print_error("MYSQL_LOG::get_log_info_for_group_id unexpected "
                        "error from get_current_log.");
        goto err1;
      }

      /*
        If the log we need to scan is the active one, protect reads with
        LOCK_log since it may also receive writes while we're scanning
        it and we don't want to get any partial events. If not active, we
        can safely read from it without any locks.
      */
      active_log= true;
      log_lock= &LOCK_log;

      break;
    }
  }

  /*
    Special case to handle group_ids set via 'SET BINLOG_GROUP_ID'. There
    is a case above which handles the scenario where no new events have
    come through the system since, but this case handles the case where
    there have been events and works because the implementation of
    update_group_id rotates to a new log when the group_id is set.

    Thus, if the matching log isn't the active log and the IDs exactly
    match. Return the end of that log.
  */
  if (!active_log && linfo->group_id == group_id_arg)
  {
    MY_STAT stat;
    if (!my_stat(linfo->log_file_name, &stat, MYF(0)))
    {
      sql_print_information("Failed to execute my_stat on file '%s'",
                            linfo->log_file_name);
      goto err1;
    }

    linfo->pos= stat.st_size;
    ret_val= 0;
    goto err1;
  }

  /*
    Found an entry which seems to match the group_id for which we're looking.
    Scan through the log searching for the matching group.
  */
  {
    File file;
    IO_CACHE log;

    if (open_binlog_with_group_id(linfo->log_file_name, log_lock, &file, &log))
      /* open_binlog_with_group_id outputs error message */
      goto err1;

    ret_val= scan_bin_log_events(&log, linfo->log_file_name,
                                 INFO_OF_ID, log_lock, &group_id_arg,
                                 &linfo->pos, &linfo->server_id);
    end_io_cache(&log);
    my_close(file, MYF(MY_WME));
  }

 err1:
  thd->current_linfo= 0;

  return ret_val;
}

/**
  Helper function to scan through a log looking at events.

  Much of this logic is duplicated from MYSQL_LOG::write_cache, but there
  are enough differences that I didn't think it was worth trying to share
  the code as it would have involved function pointers and many params.
  Currently only supports 2 modes, INFO_OF_ID and FIRST_ID_INFO. INFO_OF_ID
  mode returns the end_log_pos and server_id of the last event
  which matches group_id_arg. FIRST_ID_INFO mode doesn't currently have any
  callers but it returns the first group_id found in the log. Reads a block
  of data from cache and then parses the individual events out of the block.
  Must handle case that an event header spans 2 blocks of data. When in
  INFO_OF_ID mode, inserts appropriate events into the hierarchical
  replication cache in order to populate it.

  @param          cache          IO_CACHE from which to read
  @param          log_file_name  name of file being scanned
  @param          mode           mode of the scan, i.e. what action to take
  @param          log_lock       If non-NULL, the lock to take when reading
                                 from cache
  @param[in,out]  group_id_arg   in or out depending on mode, may receive
                                 the first group_id in the log or is the
                                 group_id for which the caller is searching
  @param[out]     pos            receives the end_log_pos of the event matching
                                 group_id_arg
  @param[out]     server_id_arg  receives the server_id of the event matching
                                 group_id_arg

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::scan_bin_log_events(IO_CACHE *cache,
                                       const char *log_file_name,
                                       enum_bin_log_scan_mode mode,
                                       pthread_mutex_t *log_lock,
                                       ulonglong *group_id_arg,
                                       my_off_t *pos, uint32 *server_id_arg)
{
  my_off_t length= my_b_bytes_in_cache(cache), carry, hdr_offs;
  Log_event_type event_type;
  my_off_t out_pos= 0;
  uint32 out_server_id= 0;
  ulonglong event_group_id_prev= 0;
  ulonglong event_group_id= 0;

  ulonglong known_cache_freq= 0;
  my_off_t cache_match_end_pos= 0;
  ulonglong cache_match_group_id= 0;
  uint32 cache_match_server_id= 0;

  /*
    Note that FORMAT_DESCRIPTION_EVENT and ROTATE_EVENT events will
    have a header size that is less than LOG_EVENT_HEADER_WITH_ID_LEN,
    but we don't have to worry about reading beyond the end of any
    buffers because the size of both FORMAT_DESCRIPTION_EVENT and
    ROTATE_EVENT are >= LOG_EVENT_HEADER_WITH_ID_LEN. ROTATE_HEADER_LEN
    is 8 and then has additional data following. The FDE is larger than
    Rotate so we're okay.
  */
  uchar header[LOG_EVENT_HEADER_WITH_ID_LEN];

  /*
    Our caller already init'ed the cache, read the FDE out of it, and
    advanced the read_pos to the start of the next event so we can just
    start reading at cache->read_pos.
  */
  hdr_offs= carry= 0;

  /* Only 2 modes currently supported */
  if (mode != INFO_OF_ID && mode != FIRST_ID_INFO)
    return 1;

  do
  {

    /*
      if we only got a partial header in the last iteration,
      get the other half now and process a full header.
    */
    if (unlikely(carry > 0))
    {
      DBUG_ASSERT(carry < LOG_EVENT_HEADER_WITH_ID_LEN);

      /* assemble both halves */
      memcpy(&header[carry], (char *) cache->read_pos,
             LOG_EVENT_HEADER_WITH_ID_LEN - carry);

      /* Get type of event */
      event_type= (Log_event_type) header[EVENT_TYPE_OFFSET];

      if (event_type != FORMAT_DESCRIPTION_EVENT &&
          event_type != ROTATE_EVENT)
      {
        if (mode == FIRST_ID_INFO)
        {
          *group_id_arg= uint8korr(&header[GROUP_ID_OFFSET]);
          *pos= uint4korr(&header[LOG_POS_OFFSET]);
          *server_id_arg= uint4korr(&header[SERVER_ID_OFFSET]);
          return 0;
        }

        DBUG_ASSERT(mode == INFO_OF_ID);

        /* Get group_id */
        event_group_id= uint8korr(&header[GROUP_ID_OFFSET]);

        if (event_group_id < event_group_id_prev)
        {
          char llbuf1[22];
          char llbuf2[22];
          snprintf(llbuf1, 22, "%llu", event_group_id_prev);
          snprintf(llbuf2, 22, "%llu", event_group_id);
          sql_print_error("MYSQL_LOG::scan_bin_log_events "
                          "encountered out of order group_ids. "
                          "prev_group_id = %s. curr_group_id = %s.",
                          llbuf1, llbuf2);
          return 1;
        }

        event_group_id_prev= event_group_id;

        /* Check if we just moved off the last event which matched. */
        if (out_pos != 0 && event_group_id > *group_id_arg)
        {
          *pos= out_pos;
          *server_id_arg= out_server_id;
          return 0;
        }

        /* Did we enter or continue within matching group? */
        else if (event_group_id == *group_id_arg)
        {
          out_pos= uint4korr(&header[LOG_POS_OFFSET]);
          out_server_id= uint4korr(&header[SERVER_ID_OFFSET]);
        }

        /* Did we fail to find a match? */
        else if (event_group_id > *group_id_arg)
        {
          return 1;
        }

        /*
          While we're scanning the log, build up the cache. Under normal
          conditions, this will not attempt to insert duplicates.
          Additionally, it will normally only 'insert' at the end of the
          vectors, not in the middle.
        */
        pthread_mutex_lock(&LOCK_log);
        /*
          We're not holding the lock for the whole operation so make
          sure things haven't changed out from under us so that we don't
          put bad data in the cache.
        */
        if (known_cache_freq == rpl_hier_cache_frequency_real)
        {
          /* Is cache even on? */
          if (rpl_hier_cache_frequency_real)
          {
            if (event_group_id &&
                !(event_group_id % rpl_hier_cache_frequency_real))
            {
              /*
                This group_id is one which should go in the cache. Save off
                it's end_log_pos.
              */
              cache_match_end_pos= uint4korr(&header[LOG_POS_OFFSET]);
              cache_match_group_id= event_group_id;
              cache_match_server_id= uint4korr(&header[SERVER_ID_OFFSET]);
            }
            else if (cache_match_end_pos != 0)
            {
              /*
                Just scanned passed the last event with an ID which should
                be in the cache. Add it.
              */
              repl_hier_cache_insert(cache_match_group_id,
                                     cache_match_server_id,
                                     log_file_name, cache_match_end_pos);
              cache_match_end_pos= 0;
            }
          }
        }
        else
        {
          /* Cache frequency changed out from under us, clear state. */
          cache_match_end_pos= 0;
        }
        known_cache_freq= rpl_hier_cache_frequency_real;
        pthread_mutex_unlock(&LOCK_log);
      }

      /* next event header at ... */
      hdr_offs= uint4korr(&header[EVENT_LEN_OFFSET]) - carry;

      carry= 0;
    }

    if (likely(length > 0))
    {
      /*
        process all event-headers in this (partial) cache.
        if next header is beyond current read-buffer,
        we'll get it later (though not necessarily in the
        very next iteration, just "eventually").
      */

      while (hdr_offs < length)
      {
        /*
          partial header only? save what we can get, process once
          we get the rest.
        */

        if (hdr_offs + LOG_EVENT_HEADER_WITH_ID_LEN > length)
        {
          carry= length - hdr_offs;
          memcpy(header, (char *) cache->read_pos + hdr_offs, carry);
          length= hdr_offs;
        }
        else
        {
          /* we've got a full event-header, and it came in one piece */

          uchar *log_pos= cache->read_pos + hdr_offs;

          /* Get type of event */
          event_type= (Log_event_type) log_pos[EVENT_TYPE_OFFSET];

          if (event_type != FORMAT_DESCRIPTION_EVENT &&
              event_type != ROTATE_EVENT)
          {
            if (mode == FIRST_ID_INFO)
            {
              *group_id_arg= uint8korr(log_pos + GROUP_ID_OFFSET);
              *pos= uint4korr(log_pos + LOG_POS_OFFSET);
              *server_id_arg= uint4korr(log_pos + SERVER_ID_OFFSET);
              return 0;
            }

            DBUG_ASSERT(mode == INFO_OF_ID);

            /* Get group_id */
            event_group_id= uint8korr(log_pos + GROUP_ID_OFFSET);

            if (event_group_id < event_group_id_prev)
            {
              char llbuf1[22];
              char llbuf2[22];
              snprintf(llbuf1, 22, "%llu", event_group_id_prev);
              snprintf(llbuf2, 22, "%llu", event_group_id);
              sql_print_error("MYSQL_LOG::scan_bin_log_events "
                              "encountered out of order group_ids. "
                              "prev_group_id = %s. curr_group_id = %s.",
                              llbuf1, llbuf2);
              return 1;
            }

            event_group_id_prev= event_group_id;

            /* Check if we just moved off the last event which matched. */
            if (out_pos != 0 && event_group_id > *group_id_arg)
            {
              *pos= out_pos;
              *server_id_arg= out_server_id;
              return 0;
            }
            /* Did we enter or continue within matching group? */
            else if (event_group_id == *group_id_arg)
            {
              out_pos= uint4korr(log_pos + LOG_POS_OFFSET);
              out_server_id= uint4korr(log_pos + SERVER_ID_OFFSET);
            }
            /* Did we fail to find a match? */
            else if (event_group_id > *group_id_arg)
            {
              return 1;
            }

            /*
              While we're scanning the log, build up the cache. Under normal
              conditions, this will not attempt to insert duplicates.
              Additionally, it will normally only 'insert' at the end of the
              vectors, not in the middle.
            */
            pthread_mutex_lock(&LOCK_log);
            /*
              We're not holding the lock for the whole operation so make
              sure things haven't changed out from under us so that we don't
              put bad data in the cache.
            */
            if (known_cache_freq == rpl_hier_cache_frequency_real)
            {
              /* Is cache even on? */
              if (rpl_hier_cache_frequency_real)
              {
                if (event_group_id &&
                    !(event_group_id % rpl_hier_cache_frequency_real))
                {
                  /*
                    This group_id is one which should go in the cache. Save off
                    it's end_log_pos.
                  */
                  cache_match_end_pos= uint4korr(log_pos + LOG_POS_OFFSET);
                  cache_match_group_id= event_group_id;
                  cache_match_server_id= uint4korr(log_pos +
                                                   SERVER_ID_OFFSET);
                }
                else if (cache_match_end_pos != 0)
                {
                  /*
                    Just scanned passed the last event with an ID which should
                    be in the cache. Add it.
                  */
                  repl_hier_cache_insert(cache_match_group_id,
                                         cache_match_server_id,
                                         log_file_name, cache_match_end_pos);
                  cache_match_end_pos= 0;
                }
              }
            }
            else
            {
              /* Cache frequency changed out from under us, clear state. */
              cache_match_end_pos= 0;
            }
            known_cache_freq= rpl_hier_cache_frequency_real;
            pthread_mutex_unlock(&LOCK_log);
          }

          /* next event header at ... */
          hdr_offs+= uint4korr(log_pos + EVENT_LEN_OFFSET);
        }
      }

      /*
        Adjust hdr_offs. Note that it may still point beyond the segment
        read in the next iteration; if the current event is very long,
        it may take a couple of read-iterations (and subsequent adjustments
        of hdr_offs) for it to point into the then-current segment.
        If we have a split header (!carry), hdr_offs will be set at the
        beginning of the next iteration, overwriting the value we set here:
      */
      hdr_offs-= length;
    }

    cache->read_pos= cache->read_end;           /* Mark buffer used up. */

    if (log_lock)
      pthread_mutex_lock(log_lock);
    length= my_b_fill(cache);
    if (log_lock)
      pthread_mutex_unlock(log_lock);
  } while (length);

  DBUG_ASSERT(carry == 0);

  /*
    The matching group_id might have been the last group in the log.
  */
  if (mode == INFO_OF_ID && out_pos != 0)
  {
    DBUG_ASSERT(event_group_id == *group_id_arg);
    *pos= out_pos;
    *server_id_arg= out_server_id;
    return 0;
  }

  /*
    Shouldn't really be here. To get here requires:
      1. Index file lists group_id >= group_id_arg for this log.
      2. No group_id >= group_id_arg found in log.
    The only way I can think of for that to happen is if group_id was
    some value a, SET BINLOG_GROUP_ID is used to override group_id to some
    new value b and then SHOW BINLOG INFO FOR is called with a value c such
    that a < c < b.
  */
  return 1;
}

/**
  Get a LOG_INFO for group_id_arg.

  @param       thd           current thd
  @param       group_id_arg  group_id to lookup
  @param[out]  linfo         receives the LOG_INFO for event which matches
                             group_id_arg

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int MYSQL_BIN_LOG::get_log_info_for_group_id(THD *thd, ulonglong group_id_arg,
                                             LOG_INFO *linfo)
{
  int ret_val= 1;
  bool used_cache= false;

  /*
    Special case for the when the request is for the current group_id.
    This is needed to cover 2 cases.
      1. A pristine system has been setup where the only events in
         any bin log are FORMAT_DESCRIPTION_EVENT and/or ROTATE_EVENTS.
      2. SET BINLOG_GROUP_ID was used and no events have come through
         the system since.
  */
  pthread_mutex_lock(&LOCK_log);
  if (group_id == group_id_arg)
  {
    ret_val= raw_get_current_log(linfo);
    pthread_mutex_unlock(&LOCK_log);
    return ret_val;
  }
  /*
    Don't bother looking through the logs if we're being asked for an ID
    from the future.
  */
  else if (group_id < group_id_arg)
  {
    pthread_mutex_unlock(&LOCK_log);
    return 1;
  }
  pthread_mutex_unlock(&LOCK_log);

   /* Use cache if enabled. */
  ret_val= cached_get_log_info_for_group_id(thd, group_id_arg,
                                            linfo, &used_cache);
  if (!used_cache)
    ret_val= uncached_get_log_info_for_group_id(thd, group_id_arg, linfo);

  return ret_val;
}

/**
  Implementation behind 'SHOW BINLOG INFO FOR' statement.

  @param  thd       current thd
  @param  group_id  group_id to lookup

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int show_binlog_info_for_group_id(THD *thd, ulonglong group_id)
{
  if (!rpl_hierarchical)
  {
    my_error(ER_OPTION_PREVENTS_STATEMENT, MYF(0), "--skip-rpl-hierarchical");
    return 1;
  }

  LOG_INFO linfo;
  if (mysql_bin_log.get_log_info_for_group_id(thd, group_id, &linfo))
  {
    my_error(ER_ERROR_WHEN_EXECUTING_COMMAND, MYF(0),
             "SHOW BINLOG INFO FOR", "Failed to find matching binlog group.");
    return 1;
  }

  List<Item> field_list;
  Protocol *protocol= thd->protocol;
  field_list.push_back(new Item_empty_string("Log_name", 255));
  field_list.push_back(new Item_return_int("Pos", 20,
                                           MYSQL_TYPE_LONGLONG));
  field_list.push_back(new Item_return_int("Server_ID", 10,
                                           MYSQL_TYPE_LONG));
  if (protocol->send_fields(&field_list,
                            Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    return 1;

  protocol->prepare_for_resend();
  int dir_len= dirname_length(linfo.log_file_name);
  protocol->store(linfo.log_file_name + dir_len,
                  strlen(linfo.log_file_name + dir_len), &my_charset_bin);

  protocol->store(linfo.pos);
  protocol->store(linfo.server_id);
  if (protocol->write())
    return 1;

  my_eof(thd);
  return 0;
}


#ifdef INNODB_COMPATIBILITY_HOOKS
/**
  Get the file name of the MySQL binlog.
  @return the name of the binlog file
*/
extern "C"
const char* mysql_bin_log_file_name(void)
{
  return mysql_bin_log.get_log_fname();
}
/**
  Get the current position of the MySQL binlog.
  @return byte offset from the beginning of the binlog
*/
extern "C"
ulonglong mysql_bin_log_file_pos(void)
{
  return (ulonglong) mysql_bin_log.get_log_file()->pos_in_file;
}
#endif /* INNODB_COMPATIBILITY_HOOKS */


struct st_mysql_storage_engine binlog_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(binlog)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &binlog_storage_engine,
  "binlog",
  "MySQL AB",
  "This is a pseudo storage engine to represent the binlog in a transaction",
  PLUGIN_LICENSE_GPL,
  binlog_init, /* Plugin Init */
  NULL, /* Plugin Deinit */
  0x0100 /* 1.0 */,
  NULL,                       /* status variables                */
  NULL,                       /* system variables                */
  NULL                        /* config options                  */
}
mysql_declare_plugin_end;
