#ifndef GOOGLESTATS_H
#define GOOGLESTATS_H

// Initialize the GoogleStats storage engine. Returns false on success.
// TODO(seanrees): remove this tentatcle from the core MySQL distro; this
// could be/should be part of stats plugin init.
extern bool googlestats_init(void);

// Shutdown the GoogleStats storage engine. Returns false on success.
extern bool googlestats_end(void);

// Used for FLUSH STATS_SERVERS. Returns false on success.
extern bool googlestats_reinit(THD*);

extern char *googlestats_servers_tbl;
extern char *googlestats_version_tbl;
extern int googlestats_timeout, googlestats_retry_interval;
extern int googlestats_log_level;
extern int googlestats_slow_threshold;
extern my_bool buffer_table_sort;
extern int googlestats_max_packet;
extern int get_versions_for_googlestats_tables(THD* thd, TABLE_LIST* tables);
extern int googlestats_show_status(THD* thd, bool verbose, const char* wild);
extern int googlestats_set_status();

enum GsLogLevel {GsLogLevelNone = 0, GsLogLevelLow, GsLogLevelMed, GsLogLevelHigh};

#endif
