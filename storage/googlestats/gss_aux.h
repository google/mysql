// Copyright 2013 Google Inc. All Rights Reserved.

#ifndef GSS_AUX_H
#define GSS_AUX_H

#include <string>
#include <vector>

#include "table.h"
#include "sql_class.h"

struct st_key_part_info;

// Get the version number and hash for each googlestats table. Return 0
// on success.
int get_versions_for_googlestats_tables(THD* thd, TABLE_LIST* tables);

class StatsServerAux
{
public:
  // Wait for data to read on sock_fd.
  // Args:
  //   sock_fd - socket to stats erver
  //   server_name - name of stats server
  //   timeout_sec - seconds to wait for data on socket
  //   wait_for_write - when true, wait for write to not block
  //   timed_out - returns true when the read timed out
  static int waitForSocket(int sock_fd, const char* server_name,
                           int timeout_sec, bool wait_for_write,
                           bool* timed_out);

  // Read up to length bytes from sock_fd. Return the number of bytes read.
  // Args:
  //   sock_fd - socket to stats erver
  //   server_name - name of stats server
  //   buf - returns data read
  //   length - amount of data to read
  //   timeout - number of seconds to block while reading
  static int readBytes(int sock_fd, const char* server_name, void* buf,
                       size_t length, int timeout);

  static void printColVal(const uchar* row, struct st_key_part_info* part_info,
    std::string* out);
  static void printColVal(Field* field, std::string* out);

  // Get the version number and hash for each googlestats table. Return 0
  // on success.
  static int getVersionNumber(THD* thd, TABLE_LIST* table);

  static void printError(const char* format, ...)
      __attribute__((format(printf, 1, 2)));

  // Wrapper for gethostbyname.
  // Args:
  //   name: hostname for which IP address is returned
  //   host: hostent struct that may be used for the result
  //   buf, buf_len: memory to use if result is large
  //
  // Returns NULL on error and a pointer to a valid hostent struct otherwise.
  //         Use the return value not the 'host' arg.
  static struct hostent* getHostByName(const char* name, struct hostent* host,
                                       char* buf, int buf_len);

  // Return the length of table_name after the date and trailing '2' characters
  // have been stripped. This is used to determine the name used to query
  // CommittedStatsVersions and LocalStatsServers. This converts table names
  // as follows:
  //   * strip a schema version (foo$20060101 -> foo)
  //   * strip trailing 2s (foo22222 -> foo)
  //   * strip trailing 2s and a schema version (foo2$20060101 -> foo)
  //   * strip anything at and after the last '$' (foo$abcd -> foo)
  // The length of the stripped string is returned.
  static int RemoveTrailing2sAndDate(const char* table_name);

  // Writes len bytes from data to fd. Returns the number of bytes written.
  // Logs an error message on failure.
  static int Write(int fd, const char* data, int len);

  // Add the backslash escape to the input string for the following character:
  //   * space, ',' , '\\', '\n'', '\r'
  // and append to an output string.
  //
  // The logic here corresponds to the logic in
  // ads/statscollection/statsserver/request-parser.cc.
  // 'key_length' is the number of bytes of 'key_buf' to escape.  The escaped
  // string is appended to 'out'.
  static void EscapeString(const char* key_buf, int key_length,
                           std::string *out);
  // Used to escape a string for an IN condition pushdown.
  static void EscapeInCondString(const char* key_buf, int key_length,
                                 std::string *out);

private:

  StatsServerAux() {}
  ~StatsServerAux() {}

  // Open and lock the version table (CommittedStatsVersions). Set *tbl and
  // *lock to 0 on failure. Return 0 on success.
  //   tbl - the opened version table
  //   lock - the lock for the opened version table
  static int lockVersionTable(const char* dbName,
                              const char* tblName,
                              TABLE** tbl,
                              MYSQL_LOCK** lock);

  // End a scan of the version table by unlocking it.
  // Parameters:
  //   tbl - the open version table
  //   lock - the lock for the open version table
  static void endVersionScan(TABLE* tbl, MYSQL_LOCK* lock);

  // Read the version number and hash for the table named by search_tbl from
  // the stats version table represented by version_tbl. Return 0 on success.
  // On success, googlestats_version_num, googlestats_version_hash and
  // googlestats_query_id are set.
  static int queryVersionNumber(THD* thd,
                                TABLE* version_tbl,
                                TABLE_LIST* search_tbl);
};


// We're turning this into a struct so we can pass it as a pointer to
// functions in ha_googlestats.h. That way we avoid having to include
// <vector> and <string> in that header file, which clashes violently
// with a bunch of mysql includes.
struct StatsServerKeyVals
{
  std::vector<std::string> vals;

  StatsServerKeyVals() : vals() {}
  StatsServerKeyVals(const StatsServerKeyVals& kvs) : vals(kvs.vals) {}
};


struct StatsServerKeys
{
  std::vector<std::vector<std::string> > vals;

  StatsServerKeys() : vals() {}
};


struct StatsServerPushedConds {
  std::vector<std::string> conds;

  StatsServerPushedConds() : conds() {}
};

#endif // GSS_AUX_H
