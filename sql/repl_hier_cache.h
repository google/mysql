/*
  Copyright (C) 2009 Google, Inc.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef REPL_HIER_CACHE_INCLUDED
#define REPL_HIER_CACHE_INCLUDED

#include "my_global.h"


/*
  This is the variable which gets updated on 'SET GLOBAL
  rpl_hierarchical_cache_frequency = ...'
*/
extern ulonglong rpl_hierarchical_cache_frequency_not_thd_safe;

/*
  This is the variable we actually use. When the above gets updated,
  This variable gets updated while being protected by mysql_bin_log.LOCK_log
  so that it's state is ensured of being consistent with whether the cache
  has been initialized.
*/
extern ulonglong rpl_hier_cache_frequency_real;

/*
  The use of STL in repl_hier_cache.cc prevents the inclusion of standard
  mysql headers in that file (e.g. sql_class.h) due to conflicts. Thus,
  there is a somewhat strange interface to the cache specified in this
  header. Specifically, the cache is poorly encapsulated. That is, calling
  code has knowledge of how the cache is implemented.

  All repl_hier_cache_* functions below require the caller to have locked
  mysql_bin_log.LOCK_log prior to being called. Header collisions prevent
  asserting this is true unfortunately.
*/

/**
  Clears all contents in the cache for all files.
*/
void repl_hier_cache_clear();

/**
  Disables (and clears) the cache. Called in error scenarios.
*/

inline void repl_hier_cache_disable()
{
  /* if the cache is working correctly, this code path should never be run */
  DBUG_ASSERT(FALSE);

  rpl_hierarchical_cache_frequency_not_thd_safe= 0;
  rpl_hier_cache_frequency_real= 0;
  repl_hier_cache_clear();
}

/**
  Tell the cache to start tracking a new bin log.

  @param  group_id       the group_id of the last event contained in the log
  @param  server_id      the server_id of the last event contained in the log
  @param  log_file_name  the log name
*/

void repl_hier_cache_append_file(ulonglong group_id, uint32 server_id,
                                 const char *log_file_name);

/**
  Tell the cache to stop tracking a bin log.

  Expects bin logs to be purged in FIFO order.

  @param  log_file_name  the log name
*/

void repl_hier_cache_purge_file(const char *log_file_name);

/**
  Appends a new cache entry into the end of the cache structures.

  @param  group_id       the group_id to append
  @param  server_id      the server_id for group_id
  @param  log_file_name  name of log containing group_id
  @param  pos            the end_log_pos of group_id
*/

void repl_hier_cache_push_back(ulonglong group_id, uint32 server_id,
                               const char *log_file_name, my_off_t pos);


/**
  Insert a new cache entry into the appropriate location.

  @param  group_id       the group_id to insert
  @param  server_id      the server_id for group_id
  @param  log_file_name  name of log containing group_id
  @param  pos            the end_log_pos of group_id
*/

void repl_hier_cache_insert(ulonglong group_id, uint32 server_id,
                            const char *log_file_name, my_off_t pos);

/**
  Updates the last known ID for the active log.

  @param  server_current_group_id      the server's current group_id
  @param  server_last_event_server_id  the server_id for above group_id
  @param  log_file_name                name of server's active log
*/

void repl_hier_cache_update_last_entry(ulonglong server_current_group_id,
                                       uint32 server_last_event_server_id,
                                       const char *log_file_name);

/**
  Given a target group_id, return the cache entry with a group_id <= target.

  @param  server_current_group_id         the server's current group_id
  @param  server_last_event_server_id     the server_id for above group_id
  @param  target_group_id                 the group_id to lookup
  @param  group_id_out                    the group_id <= target_group_id which
                                          was found in cache
  @param  server_id_out                   the server_id for group_id_out
  @param  pos_out                         the end_log_pos for group_id_out
  @param  log_file_name_out               the log name for group_id_out
  @param  returned_log_file_boundary_out  whether group_id_out is the entry
                                          marking the end of a log

  @return    Operation status
    @retval  0  success
    @retval  1  error
*/

int repl_hier_cache_lookup(ulonglong server_current_group_id,
                           uint32 server_last_event_server_id,
                           ulonglong target_group_id, ulonglong *group_id_out,
                           uint32 *server_id_out, my_off_t *pos_out,
                           const char **log_file_name_out,
                           bool *returned_log_file_boundary_out);

#endif   /* REPL_HIER_CACHE_INCLUDED */
