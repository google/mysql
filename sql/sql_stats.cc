/*
  Copyright 2011, Google Inc.
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:

      * Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
      * Redistributions in binary form must reproduce the above
  copyright notice, this list of conditions and the following disclaimer
  in the documentation and/or other materials provided with the
  distribution.
      * Neither the name of Google Inc. nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/*
  SHOW USER_STATISTICS
  * must display correct concurrent connections per account
  * must not create mutex hot spots
  * require SUPER to see stats for all users

  FLUSH USER_STATISTICS
  * remove all entries with concurrent_connections==0
  * reset all entries with concurrent_connections>0

  Create connection failure:
  * Call increment_denied_connects() to increment count of failed
    connection attempts when the account name is valid.

  Create connection:
  * Call increment_connection_count() to increment count of total and
    concurrent connections for this account.
  * call THD::cache_user_stats() to cache USER_STATS pointers

  Complete command:
  * Call update_global_user_stats() to update counts in USER_STATS entry
    in global_user_stats.

  Close connection
  * Call update_global_user_stats() to update counts in USER_STATS entry
    in global_user_stats.

  Notes:

  * global table stats are not flushed until possibly cached tables are
    closed. In many cases, this occurs at the start of the next command
    rather than the end of the current command. To compensate for this,
    some stats related commands call update_global_user_stats() prior to
    doing other work, such as displaying stats for all accounts, to
    guarantee that any work done by previous commands in the current
    THD is displayed or reset.

  * SHOW USER_STATS used to always lock LOCK_global_user_stats and
    LOCK_thread_count and then iterate over all threads to determine the
    count of concurrent connections per thread. That is a mutex hot spot.
    The current code depends on the concurrent_connections count to be
    updated on connection create and connection end. The count has been
    incremented for a THD when THD::thd_user_stats_version is != 0.
    Now, SHOW USER_STATS only takes the locks and iterates the threads
    in order to make sure that connected_time is accurate for the binlog
    dump threads and long running queries.

  * Some internal threads do not use the normal connection creation code
    path to create a thread. In that case update_global_user_stats() may
    be called with THD::thd_user_stats_version==0.
*/

#include "mysql_priv.h"

// Contains TABLE_STATS entries with db.table[.user] as the key.
HASH global_table_stats;

// Incremented when global_table_stats is flushed.
int global_table_stats_version= 0;

// Protects global table stats hash and version.
pthread_mutex_t LOCK_global_table_stats;

// Contains USER_STATS entries with account name as the key.
HASH global_user_stats;

// Incremented when global_user_stats is flushed.
int global_user_stats_version= 0;

// Protects global user stats hash and version.
pthread_mutex_t LOCK_global_user_stats;

// 'mysql_system_user' is used for when the user is not defined for a THD.
static char mysql_system_user[] = "#mysql_system#";

/**
  Returns 'user' if it's not NULL.  Returns 'mysql_system_user' otherwise.
*/

static const char *get_valid_user_string(const char *user)
{
  return user ? user : mysql_system_user;
}

/**
  strcpy which enforces max length and NULL-termination.

  Copies up to 'max_len' characters from the 'from' string to the 'to'
  string and NULL-terminates the result. The 'to' string must have room
  for at least 'max_len' + 1 characters.
*/

static void strncpy_with_nul(char *to, const char *from, size_t max_len)
{
  size_t len= min(strlen(from), max_len);
  strncpy(to, from, len);
  *(to + len)= '\0';
}

/**
  hash_get_key for table stats hash.
*/

uchar *get_key_table_stats(const TABLE_STATS *table_stats, size_t *length,
                           my_bool not_used __attribute__((unused)))
{
  *length= strlen(table_stats->table);
  return (uchar *) table_stats->table;
}

/**
  hash_free for table stats hash.
*/

void free_table_stats(TABLE_STATS *table_stats)
{
  my_free((uchar *) table_stats, MYF(0));
}

/**
  Initializes the global table stats hash.
*/

void init_global_table_stats(void)
{
  if (hash_init(&global_table_stats, system_charset_info, max_connections,
                0, 0, (hash_get_key) get_key_table_stats,
                (hash_free_key) free_table_stats, 0)) {
    sql_print_error("Initializing global_table_stats failed.");
#ifndef EMBEDDED_LIBRARY
    unireg_abort(1);
#else
    return;
#endif
  }
  global_table_stats_version++;
}

/**
  Frees the global table stats hash.
*/

void free_global_table_stats(void)
{
  hash_free(&global_table_stats);
}

/**
  Refreshes global table stats in response to a FLUSH command.
*/

void refresh_global_table_stats(void)
{
  pthread_mutex_lock(&LOCK_global_table_stats);
  free_global_table_stats();
  init_global_table_stats();
  pthread_mutex_unlock(&LOCK_global_table_stats);
}

/**
  Update global table statistics for this table.

  Will optionally update statistics for tables linked via TABLE::next.

  @param  tablep       the table for which global table stats are updated
  @param  follow_next  when true, update global stats for tables linked
                       via TABLE::next
*/

void update_table_stats(TABLE *tablep, bool follow_next)
{
  for (; tablep; tablep= tablep->next)
  {
    if (tablep->file)
      tablep->file->update_global_table_stats();

    if (!follow_next)
      return;
  }
}

/**
  Get the TABLE_STATS object for the table_name.

  Use the cached object when it is valid and update the cache fields
  when it is not.

  @param          table           table for which an object is returned
  @param[in,out]  cached_stats    pointer to TABLE_STATS object
  @param[in,out]  cached_version  value of global_table_stats_version at
                                  which cached_stats is valid.

  @return         status
    @retval       0               OK
    @retval       != 0            Error
*/

int get_table_stats(THD *thd, TABLE *table, TABLE_STATS **cached_stats,
                    int *cached_version)
{
  TABLE_STATS *table_stats;
  // [db] + '.' + [table] + [optional '.' + 'username'] + '\0'
  // If the table is a temporary table, the username is appended.
  char table_key[NAME_LEN * 2 + USER_STATS_NAME_LENGTH + 3];
  int table_key_len;
  const char *name= get_valid_user_string(thd->main_security_ctx.user);

  safe_mutex_assert_owner(&LOCK_global_table_stats);

  if (*cached_stats && *cached_version == global_table_stats_version)
    return 0;

  if (!table->s || !table->s->db.str || !table->s->table_name.str)
  {
    sql_print_error("No key for table stats.");
    return -1;
  }

  if (table->s->tmp_table == NO_TMP_TABLE)
  {
    if (snprintf(table_key, sizeof(table_key), "%s.%s", table->s->db.str,
                 table->s->table_name.str) < 3)
    {
      sql_print_error("Cannot generate name for table stats.");
      return -1;
    }
  }
  else
  {
    // Temporary tables have the username appended.
    if (snprintf(table_key, sizeof(table_key), "%s.%s.%s", table->s->db.str,
                 table->s->table_name.str, name) < 5)
    {
      sql_print_error("Cannot generate name for temp table stats.");
      return -1;
    }
  }
  table_key_len= strlen(table_key);

  // Gets or creates the TABLE_STATS object for this table.
  if (!(table_stats= (TABLE_STATS *) hash_search(&global_table_stats,
                                                 (const uchar *) table_key,
                                                 table_key_len)))
  {
    if (!(table_stats= ((TABLE_STATS *) my_malloc(sizeof(TABLE_STATS),
                                                  MYF(MY_WME)))))
    {
      // Out of memory.
      sql_print_error("Allocating table stats failed.");
      return -1;
    }
    memcpy(table_stats->table, table_key, sizeof(table_key));
    table_stats->rows_read= 0;
    table_stats->rows_changed= 0;
    table_stats->rows_changed_x_indexes= 0;

    if (my_hash_insert(&global_table_stats, (uchar *) table_stats))
    {
      // Out of memory.
      sql_print_error("Inserting table stats failed.");
      my_free((char *) table_stats, 0);
      return -1;
    }
  }
  *cached_stats= table_stats;
  *cached_version= global_table_stats_version;

  return 0;
}

/**
  hash_get_key for user stats hash.
*/

uchar *get_key_user_stats(USER_STATS *user_stats, size_t *length,
                          my_bool not_used __attribute__((unused)))
{
  *length = strlen(user_stats->user);
  return (uchar *) user_stats->user;
}

/**
  hash_free for user stats hash.
*/

void free_user_stats(USER_STATS *user_stats)
{
  my_free((uchar *) user_stats, MYF(0));
}

/**
  Initializes the global user stats hash.
*/

void init_global_user_stats(void)
{
  if (hash_init(&global_user_stats, system_charset_info, max_connections,
                0, 0, (hash_get_key) get_key_user_stats,
                (hash_free_key) free_user_stats, 0))
  {
    sql_print_error("Initializing global_user_stats failed.");
#ifndef EMBEDDED_LIBRARY
    unireg_abort(1);
#else
    return;
#endif
  }
  global_user_stats_version++;
}

/**
  Frees the global user stats hash.
*/

void free_global_user_stats(void)
{
  hash_free(&global_user_stats);
}

/**
  Returns the length of the key for a hash table of USER_STATS entries.

  The key must be small enough to fit in user_stats->user when nul-terminated
  and some of the input keys might need truncation.

  @param  name  the user name input
*/

static inline int get_user_stats_name_len(const char *name)
{
  return min(USER_STATS_NAME_LENGTH, strlen(name));
}

/**
  Get the USER_STATS object for the user associated with this THD.

  Use the cached object when it is valid. Cache the object when
  it is found.

  @param          thd     THD for which an object is returned
  @param          name    the account name

  @return         pointer to USER_STATS object, NULL when one does not exist
*/

static USER_STATS *get_user_stats(THD *thd, const char *name)
{
  int name_len= get_user_stats_name_len(name);
  safe_mutex_assert_owner(&LOCK_global_user_stats);
  if (thd->thd_user_stats_version == global_user_stats_version)
  {
    // The cached object is valid, return it.
    DBUG_ASSERT(thd->thd_user_stats);
    return thd->thd_user_stats;
  }

  USER_STATS *user_stats=
    (USER_STATS *) hash_search(&global_user_stats, (const uchar *) name,
                               name_len);
  if (user_stats)
  {
    thd->cache_user_stats(user_stats);
    return user_stats;
  }

  return NULL;
}

/**
  Initialize fields of a USER_STATS entry.
*/
void init_user_stats(USER_STATS *user_stats,
                     const char *user,
                     const char *priv_user,
                     uint total_connections,
                     uint concurrent_connections,
                     time_t connected_time,
                     double busy_time,
                     double cpu_time,
                     ulonglong bytes_received,
                     ulonglong bytes_sent,
                     ulonglong binlog_bytes_written,
                     ha_rows rows_fetched,
                     ha_rows rows_updated,
                     ha_rows rows_read,
                     ulonglong select_commands,
                     ulonglong update_commands,
                     ulonglong other_commands,
                     ulonglong commit_trans,
                     ulonglong rollback_trans,
                     ulonglong denied_connections,
                     ulonglong lost_connections,
                     ulonglong access_denied_errors,
                     ulonglong empty_queries)
{
  strncpy_with_nul(user_stats->user, user, USER_STATS_NAME_LENGTH);
  strncpy_with_nul(user_stats->priv_user, priv_user, USER_STATS_NAME_LENGTH);

  user_stats->total_connections= total_connections;
  user_stats->concurrent_connections= concurrent_connections;
  user_stats->connected_time= connected_time;
  user_stats->busy_time= busy_time;
  user_stats->cpu_time= cpu_time;
  user_stats->bytes_received= bytes_received;
  user_stats->bytes_sent= bytes_sent;
  user_stats->binlog_bytes_written= binlog_bytes_written;
  user_stats->rows_fetched= rows_fetched;
  user_stats->rows_updated= rows_updated;
  user_stats->rows_read= rows_read;
  user_stats->select_commands= select_commands;
  user_stats->update_commands= update_commands;
  user_stats->other_commands= other_commands;
  user_stats->commit_trans= commit_trans;
  user_stats->rollback_trans= rollback_trans;
  user_stats->denied_connections= denied_connections;
  user_stats->lost_connections= lost_connections;
  user_stats->access_denied_errors= access_denied_errors;
  user_stats->empty_queries= empty_queries;
}

/**
  Add an entry to global_user_stats.

  LOCK_global_user_stats must be locked when this is called.

  @param  name        user name
  @param  role_name   user's role or a bogus value
  @param  thd         THD for which entry is added
  @param  on_connect  true when called for a new connection, false
                      when called for a denied connection attempt

  @return pointer to new entry on success, NULL on failure
*/

static USER_STATS *add_user_stats_entry(const char *name, const char *role_name,
                                        THD *thd, bool on_connect)
{
  USER_STATS *user_stats;

  safe_mutex_assert_owner(&LOCK_global_user_stats);
  DBUG_ASSERT(!hash_search(&global_user_stats, (const uchar *) name,
                           get_user_stats_name_len(name)));

  // First connection for this user
  if (!(user_stats =
        ((USER_STATS *) my_malloc(sizeof(USER_STATS), MYF(MY_WME)))))
  {
    sql_print_error("add_user_stats_entry: malloc failed");
    return NULL;                                // Out of memory
  }

  init_user_stats(user_stats, name, role_name,
                  // connections
                  0, 0,
                  // time
                  0, 0, 0,
                  // bytes sent, received and written
                  0, 0, 0,
                  // rows fetched, updated and read
                  0, 0, 0,
                  // select, update and other commands
                  0, 0, 0,
                  // commit and rollback trans
                  0, 0,
                  // denied connections
                  0,
                  // lost connections
                  0,
                  // access denied errors
                  0,
                  // empty queries
                  0);

  if (my_hash_insert(&global_user_stats, (uchar *) user_stats))
  {
    sql_print_error("add_user_stats_entry: insert failed");
    my_free((char *) user_stats, 0);
    return NULL;                                // Out of memory
  }

  if (on_connect)
  {
    user_stats->total_connections= 1;
    user_stats->concurrent_connections= 1;
    thd->cache_user_stats(user_stats);
  }
  else
  {
    user_stats->denied_connections= 1;
    // Do not call THD::cache_user_stats() as concurrent_connections
    // has not been incremented.
  }

  return user_stats;
}

/**
  Used to update the global user stats.
*/

static void update_user_stats_with_user(THD *thd,
                                        USER_STATS *user_stats,
                                        time_t now)
{
  user_stats->connected_time+= now - thd->last_global_update_time;
  thd->last_global_update_time= now;
  user_stats->busy_time+= thd->diff_total_busy_time;
  user_stats->cpu_time+= thd->diff_total_cpu_time;
  user_stats->bytes_received+= thd->diff_total_bytes_received;
  user_stats->bytes_sent+= thd->diff_total_bytes_sent;
  user_stats->binlog_bytes_written+= thd->diff_total_binlog_bytes_written;
  user_stats->rows_fetched+= thd->diff_total_sent_rows;
  user_stats->rows_updated+= thd->diff_total_updated_rows;
  user_stats->rows_read+= thd->diff_total_read_rows;
  user_stats->select_commands+= thd->diff_select_commands;
  user_stats->update_commands+= thd->diff_update_commands;
  user_stats->other_commands+= thd->diff_other_commands;
  user_stats->commit_trans+= thd->diff_commit_trans;
  user_stats->rollback_trans+= thd->diff_rollback_trans;
  user_stats->denied_connections+= thd->diff_denied_connections;
  user_stats->lost_connections+= thd->diff_lost_connections;
  user_stats->access_denied_errors+= thd->diff_access_denied_errors;
  user_stats->empty_queries+= thd->diff_empty_queries;
}

/**
  Return the USER_STATS entry for the thd's user.

  Requires that LOCK_global_user_stats is locked.

  @param  thd  connection for which entry is obtained

  @return      pointer to USER_STATS object, NULL when one does not exist
*/
USER_STATS *get_user_stats_for_thd(THD *thd)
{
  const char *name= get_valid_user_string(thd->main_security_ctx.user);
  int name_len= get_user_stats_name_len(name);

  safe_mutex_assert_owner(&LOCK_global_user_stats);
  return (USER_STATS *) hash_search(&global_user_stats, (const uchar *) name,
                                    name_len);
}

/**
  Updates the global stats of a user.

  See notes on 'global table stats' in the file comment to understand why
  this is called prior to doing stats related work for some commands.

  @param  thd  connection for which stats are updated
  @param  now  current time
*/

void update_global_user_stats(THD *thd, time_t now)
{
  const char *user_string= get_valid_user_string(thd->main_security_ctx.user);

  USER_STATS *user_stats;
  pthread_mutex_lock(&LOCK_global_user_stats);

  /*
    get_user_stats() may return NULL as there are a few internal
    threads that do not go through the main connection creation code path.
  */

  // Update by user name.
  user_stats= get_user_stats(thd, user_string);
  if (!user_stats)
    user_stats= add_user_stats_entry(user_string,
                                     thd->main_security_ctx.priv_user,
                                     thd, true);
  if (user_stats)
    update_user_stats_with_user(thd, user_stats, now);
  else
    sql_print_error("update user stats failed for %s", user_string);

  thd->reset_diff_stats();
  pthread_mutex_unlock(&LOCK_global_user_stats);
}

/**
  Makes sure connection times are up-to-date for all current connections.

  Needed because mysql_binlog_send and long-running queries may have long
  intervals between calls to update_global_user_stats.
*/

void set_connections_stats(void)
{
  const time_t user_stats_full_update_max_freq= 120;
  static time_t last_update_time= 0;
  USER_STATS *user_stats;

  safe_mutex_assert_owner(&LOCK_global_user_stats);

  time_t now = time(NULL);
  if (now - last_update_time > user_stats_full_update_max_freq)
  {
    pthread_mutex_lock(&LOCK_thread_count);

    I_List_iterator<THD> it(threads);
    THD *thd;
    /* Iterates through the current threads. */
    while ((thd= it++))
    {
      if (thd->thd_user_stats_version == 0)
      {
        /*
          Do nothing as the count was never incremented for this THD.

          This check is the same check which is done in
          decrement_connection_count. We don't want to take any action
          if the condition is met because get_user_stats would end up
          setting thd_user_stats_version by calling cache_user_stats.
          thd_user_stats_version doesn't typically get set until
          check_user is called during connect. However, if the connection
          fails due to ER_HANDSHAKE_ERROR or a password error, then
          check_user won't call increment_connection_count, and our setting
          thd_user_stats_version here would cause decrement to try to do
          work and the connection count to go negative.
        */
        continue;
      }

      const char *name= get_valid_user_string(thd->main_security_ctx.user);

      /*
        get_user_stats() may return NULL as there are a few internal
        threads that do not go through the main connection creation code path.
        We do not update stats for those threads to try to keep the time spent
        in the loop down.
      */

      /* Update by user name. */
      user_stats= get_user_stats(thd, name);
      if (user_stats != NULL)
      {
        update_user_stats_with_user(thd, user_stats, now);
        thd->reset_diff_stats();
      }
    }

    pthread_mutex_unlock(&LOCK_thread_count);
    last_update_time= now;
  }
}

/**
  Reset all fields except concurrent_connections to 0.

  This function is called for entries in global_user_stats. We depend on
  an accurate count of concurrent_connections per account, so that field
  is never reset. It is incremented and decremented as connections come
  and go.

  @param  user_stats  the user stats entry to reset
*/

static void reset_user_stats(USER_STATS *user_stats)
{
  user_stats->total_connections= 0;
  /* DO NOT RESET -- user_stats->concurrent_connections */
  user_stats->connected_time=0;
  user_stats->busy_time= 0;
  user_stats->cpu_time= 0;
  user_stats->bytes_received= 0;
  user_stats->bytes_sent= 0;
  user_stats->binlog_bytes_written= 0;
  user_stats->rows_fetched= 0;
  user_stats->rows_updated= 0;
  user_stats->rows_read= 0;
  user_stats->select_commands= 0;
  user_stats->update_commands= 0;
  user_stats->other_commands= 0;
  user_stats->commit_trans= 0;
  user_stats->rollback_trans= 0;
  user_stats->denied_connections= 0;
  user_stats->lost_connections= 0;
  user_stats->access_denied_errors= 0;
  user_stats->empty_queries= 0;
}

/**
  Resets the hash table of global user statistics.

  This function removes all entries for which
  USER_STATS::concurrent_connections==0 and clears all fields except
  concurrent_connections for the remaining entries. LOCK_global_user_stats
  must be held when this is called.

  @param  stats_hash     hash table of USER_STATS for global user statistics
  @param  stats_version  pointer to global_user_stats_version

  @return         status
    @retval       0               OK
    @retval       != 0            Error
*/

int reset_global_user_stats(HASH *stats_hash, int *stats_version)
{
  DYNAMIC_ARRAY delete_items;
  safe_mutex_assert_owner(&LOCK_global_user_stats);

  if (my_init_dynamic_array(&delete_items, sizeof(USER_STATS *),
                            stats_hash->records, 1))
  {
    sql_print_error("reset global stats: cannot initialize dynamic array");
    my_message(ER_OUTOFMEMORY, ER(ER_OUTOFMEMORY), MYF(0));
    return 1;
  }

  for (ulong i= 0; i < stats_hash->records; ++i)
  {
    USER_STATS *entry= (USER_STATS *) hash_element(stats_hash, i);
    if (entry->concurrent_connections)
    {
      reset_user_stats(entry);
    }
    else
    {
      if (insert_dynamic(&delete_items, (uchar *) &entry))
      {
        delete_dynamic(&delete_items);
        sql_print_error("reset global stats: insert_dynamic failed");
        my_message(ER_OUTOFMEMORY, ER(ER_OUTOFMEMORY), MYF(0));
        return 1;
      }
    }
  }

  for (uint i= 0; i < delete_items.elements; ++i)
  {
    USER_STATS **entryp= (USER_STATS **) dynamic_array_ptr(&delete_items, i);
    my_bool r= hash_delete(stats_hash, (uchar *) *entryp);
    if (r)
#ifndef EMBEDDED_LIBRARY
      unireg_abort(1);
#else
      return 1;
#endif
  }
  delete_dynamic(&delete_items);
  (*stats_version)++;
  return 0;
}

/**
  Refreshes global user stats in response to a FLUSH command.
*/

int refresh_global_user_stats(THD *thd)
{
  int result= 0;

  update_global_user_stats(thd, time(NULL));

  pthread_mutex_lock(&LOCK_global_user_stats);
  if (reset_global_user_stats(&global_user_stats, &global_user_stats_version))
    result = 1;
  pthread_mutex_unlock(&LOCK_global_user_stats);
  return result;
}

/**
  Increments the global user concurrent connection count.

  LOCK_global_user_stats is locked/unlocked inside this function.

  @param  thd  update count for user authenticated in this session

  @return         status
    @retval       0               OK
    @retval       != 0            Error
*/

int increment_connection_count(THD *thd)
{
  const char *user_string= get_valid_user_string(thd->main_security_ctx.user);
  int return_value= 0;

  pthread_mutex_lock(&LOCK_global_user_stats);

  USER_STATS *user_stats= get_user_stats(thd, user_string);
  if (user_stats != NULL)
  {
    user_stats->total_connections++;
    user_stats->concurrent_connections++;
  }
  else if (!(user_stats=
             add_user_stats_entry(user_string,
                                  thd->main_security_ctx.priv_user,
                                  thd, true)))
  {
    return_value= 1;
  }

  pthread_mutex_unlock(&LOCK_global_user_stats);
  return return_value;
}

/**
  Increment count of denied connections.

  USER_STATS entries are added when none are found in global_user_stats
  as this is only called when the THD uses a valid account name.

  @param  thd  describes account and (IP or hostname) to increment
*/

void increment_denied_connects(THD *thd)
{
  const char *name= get_valid_user_string(thd->main_security_ctx.user);
  int name_len= get_user_stats_name_len(name);

  /* priv_user may not be set since the connection was denied. */
  const char *role_name= thd->main_security_ctx.priv_user ?
    thd->main_security_ctx.priv_user : "";

  pthread_mutex_lock(&LOCK_global_user_stats);

  USER_STATS *user_stats=
      (USER_STATS *) hash_search(&global_user_stats, (const uchar *) name,
                                 name_len);
  if (!user_stats)
    user_stats= add_user_stats_entry(name, role_name, thd, false);
  else
    user_stats->denied_connections++;

  pthread_mutex_unlock(&LOCK_global_user_stats);
}

/**
  Decrements the global user concurrent connection count.

  LOCK_global_user_stats is locked/unlocked inside this function.

  @param  thd  account for which the count is decremented
*/

void decrement_connection_count(THD *thd)
{
  USER_STATS *user_stats= 0;

  pthread_mutex_lock(&LOCK_global_user_stats);

  if (thd->thd_user_stats_version == 0)
  {
    // Do nothing as the count was never incremented for this THD.
  }
  else if (thd->thd_user_stats_version == global_user_stats_version)
  {
    user_stats= thd->thd_user_stats;
  }
  else
  {
    const char *name= get_valid_user_string(thd->main_security_ctx.user);
    user_stats= (USER_STATS *) hash_search(&global_user_stats,
                                           (const uchar *) name,
                                           get_user_stats_name_len(name));
  }

  if (user_stats)
  {
    if (user_stats->concurrent_connections == 0)
    {
      sql_print_error("Cannot decrement concurrent_connections for %s. "
                      "Will intentionally KILL the process.",
                      get_valid_user_string(thd->main_security_ctx.user));
      /*
        Used to use unireg_abort() here, but found that it caused deadlocks
        when initiated from handle_slave_io.

        A unireg_abort call ends up resulting in a call to InnoDB's
        logs_empty_and_mark_files_at_shutdown which will sit in a loop
        until there are no pending transactions. However, when
        called here from handle_slave_io, the thread owns LOCK_thread_count,
        which prevents any threads from being created or destroyed and so
        any pending transactions stay that way forever.
      */
      kill(getpid(), SIGKILL);
    }
    --(user_stats->concurrent_connections);
  }

  pthread_mutex_unlock(&LOCK_global_user_stats);
}
