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

#include "mysql_priv.h"

// Contains TABLE_STATS entries with db.table[.user] as the key.
HASH global_table_stats;

// Incremented when global_table_stats is flushed.
int global_table_stats_version= 0;

// Protects global table stats hash and version.
pthread_mutex_t LOCK_global_table_stats;

// 'mysql_system_user' is used for when the user is not defined for a THD.
static char mysql_system_user[] = "#mysql_system#";

/**
  Returns 'user' if it's not NULL.  Returns 'mysql_system_user' otherwise.
*/

static char *get_valid_user_string(char *user)
{
  return user ? user : mysql_system_user;
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
    unireg_abort(1);
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
  TABLE_STATS* table_stats;
  // [db] + '.' + [table] + [optional '.' + 'username'] + '\0'
  // If the table is a temporary table, the username is appended.
  char table_key[NAME_LEN * 2 + USER_STATS_NAME_LENGTH + 3];
  int table_key_len;
  char *name= get_valid_user_string(thd->main_security_ctx.user);

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
                                                 (uchar *) table_key,
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
