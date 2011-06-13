/* Copyright (C) 2009 Google, Inc.

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

#include <algorithm>
#include <deque>
#include <string>
#include <vector>

#include <string.h>
#include "repl_hier_cache.h"

ulonglong rpl_hierarchical_cache_frequency_not_thd_safe;
ulonglong rpl_hier_cache_frequency_real= 0;

/* Declared in mysql_priv.h, but we can't include that here */
extern void sql_print_error(const char *format, ...);

/*
  Goal of the cache is to reduce the amount of sequential scanning of bin
  logs which needs to be done to handle 'SHOW BINLOG INFO FOR' and a slave
  connection using its group_id. Thus, the cache doesn't keep track of
  specific group_ids which have been requested and return those. Rather,
  it keeps track of the end_log_pos of every N group_ids. When a request comes
  in for group_id Y, the cache finds group_id X such that X <= Y and returns
  that information. Using that, the sequential scanning code can seek
  the binlog to a known position and start from there rather than having to
  start at the beginning.

  Implementation of the cache is as a vector of vectors. Top-level vector
  records contain
    ulonglong     group_id,
    std::string   log_file_name
    vector<>      group_id_to_pos_mapping, sorted by group_id

  As implied by the name, the group_id_to_pos_mapping vector elements contain
  a group_id and its corresponding end_log_pos in the the bin log.

  Entries in the top-level vector store the last known group_id contained in
  the corresponsding bin log, similar to what is stored in the index.
  So, the group_ids in the cache are ordered something like this:

              +--------+--------+--------+--------+
              | 1000   | 2000   | 3000   | 3682   |
              |        |        |        |        |
              |repl.01 |repl.02 |repl.03 |repl.04 |
              +------|-+------|-+------|-+------|-+
                     |        |        |        |
                     |        |        |        |
               +-----+  +-----+  +-----+  +-----+
               |        |        |        |
               |        |        |        |
               \/       \/       \/       \/
               +-----+  +-----+  +-----+  +-----+
               | 200 |  | 1200|  | 2200|  | 3200|
               +-----+  +-----+  +-----+  +-----+
               | 400 |  | 1400|  | 2400|  | 3400|
               +-----+  +-----+  +-----+  +-----+
               | 600 |  | 1600|  | 2600|  | 3600|
               +-----+  +-----+  +-----+  +-----+
               | 800 |  | 1800|  | 2800|
               +-----+  +-----+  +-----+

  If cache is enabled, top-level vector is always populated. It is initialized
  at server start and at runtime when the cache switches from disabled to
  enabled. The group_id_to_pos mapping vectors are populated as new events
  are written to the binlog and by the lookup's sequential scanning code if
  it hits a group_id which should be in the cache (but apparently isn't
  or else the sequential scanning code wouldn't be processing it).

  This implementation is more complex than a single, flat vector, but has a
  few advantages:
    Each bin log name is only stored a single time.
    Easier to erase all entries for a given bin log when it is purged.
    Normal usage patterns will typically yield inserts only at the end of
      the vectors, which are O(1) rather than inserts in the middle which
      are O(n). The case which would cause inserts into the middle is
      changing the value of rpl_hierarchical_cache_frequency at runtime
      to a new value without first clearing the cache.
*/


class ID_TO_POS_ENTRY
{
 public:
  ID_TO_POS_ENTRY(ulonglong group_id, uint32 server_id, my_off_t pos)
    :group_id(group_id), server_id(server_id), pos(pos)
  {}

  /* required in order to use std::lower_bound */
  bool operator < (const ID_TO_POS_ENTRY &other) const
  {
    return group_id < other.group_id;
  }

  ulonglong   group_id;
  uint32      server_id;
  my_off_t    pos;
};


class REPL_HIER_CACHE_ENTRY
{
 public:
  REPL_HIER_CACHE_ENTRY(ulonglong group_id, uint32 server_id,
                        const char *log_file_name)
    :group_id(group_id), server_id(server_id), log_file_name(log_file_name),
     id_to_pos()
  {}

  /* required in order to use std::lower_bound */
  bool operator < (const REPL_HIER_CACHE_ENTRY &other) const
  {
    return group_id < other.group_id;
  }

  ulonglong                     group_id;
  uint32                        server_id;
  std::string                   log_file_name;
  std::vector<ID_TO_POS_ENTRY>  id_to_pos;
};


std::deque<REPL_HIER_CACHE_ENTRY> repl_hier_cache;


#ifndef DBUG_OFF
void repl_hier_cache_assert_valid()
{
  std::deque<REPL_HIER_CACHE_ENTRY>::iterator i;
  std::vector<ID_TO_POS_ENTRY>::iterator j;
  int index;

  for (i= repl_hier_cache.begin(); i != repl_hier_cache.end(); ++i)
  {
    if ((index= (i - repl_hier_cache.begin())))
      DBUG_ASSERT(repl_hier_cache[index-1].group_id <= i->group_id);

    std::vector<ID_TO_POS_ENTRY> *id_to_pos= &i->id_to_pos;
    for (j= id_to_pos->begin(); j != id_to_pos->end(); ++j)
    {
      /* i has the ID of the last event in the file */
      DBUG_ASSERT(i->group_id >= j->group_id);

      if ((index= (j - id_to_pos->begin())))
      {
        DBUG_ASSERT((*id_to_pos)[index-1].group_id < j->group_id);
        DBUG_ASSERT((*id_to_pos)[index-1].pos < j->pos);
      }
    }
  }
}
#endif


/**
  Clears all contents in the cache for all files.
*/

void repl_hier_cache_clear()
{
  repl_hier_cache.clear();

#ifndef DBUG_OFF
  repl_hier_cache_assert_valid();
#endif
}


/**
  Tell the cache to start tracking a new bin log.

  @param  group_id       the group_id of the last event contained in the log
  @param  server_id      the server_id of the last event contained in the log
  @param  log_file_name  the log name
*/

void repl_hier_cache_append_file(ulonglong group_id, uint32 server_id,
                                 const char *log_file_name)
{
  /*
    If we have entries, the entry to be appended cannot have a lower
    group_id than the current last entry.

    Note that adjacent entries may have the same group_id if the only events
    in a bin log are Format_description_log_event and/or Rotate_log_event.
  */
  if (repl_hier_cache.size() &&
      group_id < repl_hier_cache.back().group_id)
  {
    char llbuf1[22], llbuf2[22];
    snprintf(llbuf1, 22, "%llu", (unsigned long long) group_id);
    snprintf(llbuf2, 22, "%llu",
             (unsigned long long) repl_hier_cache.back().group_id);
    sql_print_error("repl_hier_cache trying to append out of order group_id "
                    "(%s, %s). Last entry in cache is currently (%s, %s). "
                    "Disabling cache.", log_file_name, llbuf1,
                    repl_hier_cache.back().log_file_name.c_str(), llbuf2);
    repl_hier_cache_disable();
    return;
  }

  REPL_HIER_CACHE_ENTRY new_entry(group_id, server_id, log_file_name);
  repl_hier_cache.push_back(new_entry);

#ifndef DBUG_OFF
  repl_hier_cache_assert_valid();
#endif
}


/**
  Tell the cache to stop tracking a bin log.

  Expects bin logs to be purged in FIFO order.

  @param  log_file_name  the log name
*/

void repl_hier_cache_purge_file(const char *log_file_name)
{
  if (!repl_hier_cache.size())
  {
    sql_print_error("repl_hier_cache attempting to purge %s when cache is "
                    "empty. Disabling cache.", log_file_name);
    repl_hier_cache_disable();
    return;
  }

  if (strcmp(log_file_name,
             repl_hier_cache.front().log_file_name.c_str()))
  {
    sql_print_error("repl_hier_cache attempting to purge %s when %s is first "
                    "in cache. Disabling cache.", log_file_name,
                    repl_hier_cache.front().log_file_name.c_str());
    repl_hier_cache_disable();
    return;
  }

  repl_hier_cache.pop_front();

#ifndef DBUG_OFF
  repl_hier_cache_assert_valid();
#endif
}


/**
  Appends a new cache entry into the end of the cache structures.

  @param  group_id       the group_id to append
  @param  server_id      the server_id for group_id
  @param  log_file_name  name of log containing group_id
  @param  pos            the end_log_pos of group_id
*/

void repl_hier_cache_push_back(ulonglong group_id, uint32 server_id,
                               const char *log_file_name, my_off_t pos)
{
  if (!repl_hier_cache.size())
  {
    char llbuf[22];
    snprintf(llbuf, 22, "%llu", (unsigned long long) group_id);
    sql_print_error("repl_hier_cache attempting to push_back (%s, %s) when "
                    "cache is empty. Disabling cache.", log_file_name, llbuf);
    repl_hier_cache_disable();
    return;
  }

  if (strcmp(log_file_name,
             repl_hier_cache.back().log_file_name.c_str()))
  {
    char llbuf[22];
    snprintf(llbuf, 22, "%llu", (unsigned long long) group_id);
    sql_print_error("repl_hier_cache attempting to push_back (%s, %s) with "
                    "wrong log file. Expecting %s. Disabling cache.",
                    log_file_name, llbuf,
                    repl_hier_cache.back().log_file_name.c_str());
    repl_hier_cache_disable();
    return;
  }

  std::vector<ID_TO_POS_ENTRY> *id_to_pos= &repl_hier_cache.back().id_to_pos;

  if (id_to_pos->size() && group_id <= id_to_pos->back().group_id)
  {
    char llbuf1[22], llbuf2[22];
    snprintf(llbuf1, 22, "%llu", (unsigned long long) group_id);
    snprintf(llbuf2, 22, "%llu",
             (unsigned long long) id_to_pos->back().group_id);
    sql_print_error("repl_hier_cache trying to push_back out of order "
                    "group_id (%s, %s). Last entry in cache is currently "
                    "(%s, %s). Disabling cache.", log_file_name, llbuf1,
                    repl_hier_cache.back().log_file_name.c_str(), llbuf2);
    repl_hier_cache_disable();
    return;
  }

  /* Record that the last known ID has changed. */
  repl_hier_cache.back().group_id= group_id;

  ID_TO_POS_ENTRY new_entry(group_id, server_id, pos);
  id_to_pos->push_back(new_entry);

#ifndef DBUG_OFF
  repl_hier_cache_assert_valid();
#endif
}


/**
  Insert a new cache entry into the appropriate location.

  @param  group_id       the group_id to insert
  @param  server_id      the server_id for group_id
  @param  log_file_name  name of log containing group_id
  @param  pos            the end_log_pos of group_id
*/

void repl_hier_cache_insert(ulonglong group_id, uint32 server_id,
                            const char *log_file_name, my_off_t pos)
{
  if (!repl_hier_cache.size())
  {
    char llbuf[22];
    snprintf(llbuf, 22, "%llu", (unsigned long long) group_id);
    sql_print_error("repl_hier_cache attempting to insert (%s, %s) when "
                    "cache is empty. Disabling cache.", log_file_name, llbuf);
    repl_hier_cache_disable();
    return;
  }

  REPL_HIER_CACHE_ENTRY dummy(group_id, 0, "");

  std::deque<REPL_HIER_CACHE_ENTRY>::iterator it1;
  it1= std::lower_bound(repl_hier_cache.begin(), repl_hier_cache.end(),
                        dummy);

  /* Inserts shouldn't be beyond the current end. */
  if (it1 == repl_hier_cache.end())
  {
    char llbuf1[22], llbuf2[22];
    snprintf(llbuf1, 22, "%llu", (unsigned long long) group_id);
    snprintf(llbuf2, 22, "%llu",
             (unsigned long long) repl_hier_cache.back().group_id);
    sql_print_error("repl_hier_cache attempting to insert (%s, %s) beyond "
                    "the current end (%s, %s). Disabling cache.",
                    log_file_name, llbuf1,
                    repl_hier_cache.back().log_file_name.c_str(), llbuf2);
    repl_hier_cache_disable();
    return;
  }

  /*
    Requested group_id should be less than or equal to the group_id at
    the returned position and log_file_name should match.
  */
  DBUG_ASSERT(group_id <= it1->group_id);
  DBUG_ASSERT(!strcmp(log_file_name, it1->log_file_name.c_str()));

  std::vector<ID_TO_POS_ENTRY>::iterator it2;
  ID_TO_POS_ENTRY new_entry(group_id, server_id, pos);
  it2= std::lower_bound(it1->id_to_pos.begin(), it1->id_to_pos.end(),
                        new_entry);

  /*
    Don't insert a duplicate.
  */
  if (it2 == it1->id_to_pos.end() || new_entry.group_id != it2->group_id)
    it1->id_to_pos.insert(it2, new_entry);

#ifndef DBUG_OFF
  repl_hier_cache_assert_valid();
#endif
}


/**
  Updates the last known ID for the active log.

  @param  server_current_group_id      the server's current group_id
  @param  server_last_event_server_id  the server_id for above group_id
  @param  log_file_name                name of server's active log
*/

void repl_hier_cache_update_last_entry(ulonglong server_current_group_id,
                                       uint32 server_last_event_server_id,
                                       const char *log_file_name)
{
  if (!repl_hier_cache.size())
  {
    char llbuf[22];
    snprintf(llbuf, 22, "%llu", (unsigned long long)server_current_group_id);
    sql_print_error("repl_hier_cache attempting to update last (%s, %s) when "
                    "cache is empty. Disabling cache.", log_file_name, llbuf);
    repl_hier_cache_disable();
    return;
  }

  if (server_current_group_id < repl_hier_cache.back().group_id ||
      strcmp(repl_hier_cache.back().log_file_name.c_str(), log_file_name))
  {
    char llbuf1[22], llbuf2[22];
    snprintf(llbuf1, 22, "%llu", (unsigned long long)server_current_group_id);
    snprintf(llbuf2, 22, "%llu",
             (unsigned long long) repl_hier_cache.back().group_id);
    sql_print_error("repl_hier_cache attempting to update last (%s, %s), "
                    "but incompatible with current last (%s, %s). Disabling "
                    "cache.", log_file_name, llbuf1,
                    repl_hier_cache.back().log_file_name.c_str(), llbuf2);
    repl_hier_cache_disable();
    return;
  }

  repl_hier_cache.back().group_id= server_current_group_id;
  repl_hier_cache.back().server_id= server_last_event_server_id;

#ifndef DBUG_OFF
  repl_hier_cache_assert_valid();
#endif
}


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
                           bool *returned_log_file_boundary_out)
{
  int ret_val= 1;                               /* failure */

  *group_id_out= 0;
  *server_id_out= 0;
  *pos_out= 0;
  *log_file_name_out= NULL;
  *returned_log_file_boundary_out= FALSE;

  if (repl_hier_cache.size())
  {
    /*
      Server could have consumed more IDs since cache was last updated.
      Before doing a lookup, update cache to know server's current ID.
    */
    if (server_current_group_id < repl_hier_cache.back().group_id)
    {
      char llbuf1[22], llbuf2[22];
      snprintf(llbuf1, 22, "%llu",
               (unsigned long long)server_current_group_id);
      snprintf(llbuf2, 22, "%llu",
               (unsigned long long) repl_hier_cache.back().group_id);
      sql_print_error("repl_hier_cache lookup attempting to update last %s, "
                      "but incompatible with current last %s. Disabling "
                      "cache.", llbuf1, llbuf2);
      repl_hier_cache_disable();
      return 1;
    }

    repl_hier_cache.back().group_id= server_current_group_id;
    repl_hier_cache.back().server_id= server_last_event_server_id;

    /* Find the entry for the log containing target ID. */
    REPL_HIER_CACHE_ENTRY dummy1(target_group_id, 0, "");
    std::deque<REPL_HIER_CACHE_ENTRY>::iterator it1;
    it1= std::lower_bound(repl_hier_cache.begin(), repl_hier_cache.end(),
                          dummy1);

    /*
      Check that target ID is not beyond known range.
    */
    if (it1 != repl_hier_cache.end())
    {
      std::vector<ID_TO_POS_ENTRY> *id_to_pos= &it1->id_to_pos;

      /*
        Target group_id should either be less than or equal to the
        group_id at the returned position.
      */
      DBUG_ASSERT(target_group_id <= it1->group_id);

      /*
        There are multiple conditions under which we want to just return
        as much info as we have from the top-level entry and then let the
        caller decide the action to take.
          1. target matches entry's value.
          2. entry doesn't have any second level entries.
          3. target is less than all second level entries.
      */
      if (target_group_id == it1->group_id ||
          !id_to_pos->size() ||
          target_group_id < id_to_pos->front().group_id)
      {
        *group_id_out= it1->group_id;
        *server_id_out= it1->server_id;

        /*
          In this case, we want to use the log's size as the pos, but
          due to header conflicts we can't call my_stat here. Instead,
          pass flag back to caller telling them to do it.
        */
        *returned_log_file_boundary_out= TRUE;
      }
      else
      {
        std::vector<ID_TO_POS_ENTRY>::iterator it2;
        ID_TO_POS_ENTRY dummy2(target_group_id, 0, 0);
        it2= std::lower_bound(id_to_pos->begin(), id_to_pos->end(),
                              dummy2);

        /*
          Should only have gotten begin on an exact match due to
          check done above.
        */
        DBUG_ASSERT(it2 != id_to_pos->begin() ||
                    target_group_id == it2->group_id);
        /*
          lower_bound returns the position at which a value should be
          inserted in order to keep the sequence sorted. However, that
          isn't exactly what we want. When the requested value doesn't
          exist, we want the entry with a lower group_id, but we're getting
          the entry just after that. For example, assume entries:

              +-----+-----+-----+
              | 105 | 110 | 115 |
              +-----+-----+-----+

          If the requested value is 113, lower_bound returns entry with
          index 2 (i.e value 115), but the entry we want has index 1.
        */
        DBUG_ASSERT(it2 == id_to_pos->end() ||
                    target_group_id <= it2->group_id);

        if (it2 == id_to_pos->end() || target_group_id < it2->group_id)
        {
          --it2;

          /* it2 must now be less than requested value */
          DBUG_ASSERT(it2->group_id < target_group_id);
        }

        DBUG_ASSERT(it2->group_id <= target_group_id);
#ifndef DBUG_OFF
        std::vector<ID_TO_POS_ENTRY>::iterator dbug_b4, dbug_after;
        dbug_b4= dbug_after= it2;
        --dbug_b4; ++dbug_after;
#endif
        DBUG_ASSERT(it2 == id_to_pos->begin() ||
                    dbug_b4->group_id < target_group_id);
        DBUG_ASSERT(dbug_after == id_to_pos->end() ||
                    target_group_id < dbug_after->group_id);

        *group_id_out= it2->group_id;
        *server_id_out= it2->server_id;
        *pos_out= it2->pos;
      }

      *log_file_name_out= it1->log_file_name.c_str();

      ret_val= 0;
    }
  }

  return ret_val;
}
