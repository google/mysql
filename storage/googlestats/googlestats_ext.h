// Copyright 2013 Google Inc. All Rights Reserved.

#ifndef GOOGLESTATS_EXT_H
#define GOOGLESTATS_EXT_H

extern int get_versions_for_googlestats_tables(THD* thd, TABLE_LIST* tables);

extern bool googlestats_reinit(THD*);
extern int googlestats_show_status(THD* thd, bool verbose, const char* wild);
extern int googlestats_set_status();

#endif
