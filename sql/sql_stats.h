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

#ifndef SQL_STATS_INCLUDED
#define SQL_STATS_INCLUDED

// Variables for table statistics.
extern HASH global_table_stats;
extern int global_table_stats_version;
extern pthread_mutex_t LOCK_global_table_stats;

// Variables for user statistics.
extern HASH global_user_stats;
extern int global_user_stats_version;
extern pthread_mutex_t LOCK_global_user_stats;

// Functions for table statistics.
void init_global_table_stats(void);
void free_global_table_stats(void);
void refresh_global_table_stats(void);

void update_table_stats(TABLE *table_ptr, bool follow_next);
int get_table_stats(THD *thd, TABLE *table, TABLE_STATS **cached_stats,
                    int *cached_version);

// Functions for user statistics.
void init_global_user_stats(void);
void free_global_user_stats(void);
int refresh_global_user_stats(THD *thd);

USER_STATS *get_user_stats_for_thd(THD *thd);
void update_global_user_stats(THD *thd, time_t now);
void set_connections_stats(void);

int increment_connection_count(THD *thd);
void increment_denied_connects(THD *thd);
void decrement_connection_count(THD *thd);

#endif /* SQL_STATS_INCLUDED */
