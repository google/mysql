// Copyright 2013 Google Inc. All Rights Reserved.

#ifndef GSS_CACHE_H
#define GSS_CACHE_H

#include <my_pthread.h>
#include <time.h>
#include <netinet/in.h>

#include "table.h"
#include "mysqld_error.h"
#include "log.h"

class StatsServerCache;

const int kMaxStatsServerTier = 9;

// State used when calling StatsServerCache::connect multiple times to get
// a connection and the caller wants to avoid using (and killing) the same
// server more than twice.
//
// Initially, tier is 0 and this searches for a server starting at tier 0.
// Each tier is searched until a server is found or all tiers (0 - 9) are
// tried. When this occurs, if hasRetried is false, it is set to true, tier
// is reset to 0 and the search is done again. On the second round of
// searching the state of the server (alive/dead) and table (good/bad) are
// ignored.
//
// The index into the vectors of servers for a given entry in
// StatsServerCache::tblMap is computed as:
//   (currentOffset + startIndex) % servers.size()
class StatsServerConnectState {
 public:
  StatsServerConnectState()
      : tier(0),
        searchTier(0),
        startIndex(-1),
        currentOffset(-1),
        hasRetried(false),
        foundMatch(false)
  { }

  // Return true when no more connections can be obtained.
  bool getIsDone() const {
    return (hasRetried && tier > kMaxStatsServerTier);
  }

  bool getHasRetried() const { return hasRetried; }
  int getTier() const { return tier; }

 private:
  int tier;          // current tier
  int searchTier;    // tier to search for a server
  int startIndex;    // first index into vector of servers for current tier
  int currentOffset; // current index into vector of servers for current tier
  bool hasRetried;   // true after the first pass through the set of servers
  bool foundMatch;   // true after at least 1 server to connect to was found

  // This must also prepare the object for the second pass through the set
  // of possible servers.
  void setHasRetried() {
    hasRetried = true;
    startIndex = currentOffset = -1;
    tier = 0;
    searchTier = 0;
  }

  friend class StatsServerCache;
};

// The cache maps a key to a vector of stats servers. The vector contains
// stats servers that serve data for that key. The key is (name, tier) where
// tier is between 0 and 9 and indicates the distance between the database
// and stats server. 0 is the closest. name is one of: 'db', 'db.table' or
// 'db.table.index':
//   * 'db'
//       ** sometimes called a wildcard server
//       ** serves all tables for the database 'db'
//   * 'db.table'
//       ** an entry that is sharded by table
//       ** serves the table 'table' for the database 'db'
//   * 'db.table.index'
//       ** an entry that is sharded by index
//       ** serves the index 'index' for the table 'table' in the database 'db'
//
// The STL map used for the cache is populated so that the map entry (a vector)
// for the key ('db.table.index', tier) contains (is a superset of) all stats
// servers from the map entries for the keys ('db.table', tier) and
// ('db', tier). Also, the map entry for the key ('db.table', tier) contains
// all stats servers from the map entry for the key ('db', tier).
//
// This is done so that one lookup of the cache returns all stats servers that
// can be used. The server that will be used is then selected randomly.

class StatsServerCache
{
public:
  // See ::_connect() for more comments. The 'state' argument allows the caller
  // to avoid connecting to the same server more than twice. This is important
  // because there might be a query of death that kills the server.
  static int connect(const char* dbName,
                     const char* tableName,
                     const char* indexName,
                     std::string* serverName,
                     int* port,
                     StatsServerConnectState* state,
                     int* errorCode)
  {
    return(instance()->_connect(dbName, tableName, indexName, serverName,
                                port, state, errorCode));
  }

  // See ::_markTableBadAtServer() for comments.
  static void markTableBadAtServer(const char* dbName,
                                   const char* tableName,
                                   const char* indexName,
                                   const char* serverName,
                                   int port)
  {
    return(instance()->_markTableBadAtServer(dbName, tableName, indexName,
                                             serverName, port));
  }

  // See ::_reinit for comments.
  static bool reinit(THD* thd)
  {
    if (instance())
    {
      return instance()->_reinit(thd);
    }
    else
    {
      my_message(ER_UNKNOWN_ERROR, "GoogleStats not initialized.", MYF(0));
      return false;
    }
  }

  // See ::_showStatus for comments.
  static int showStatus(THD* thd, bool verbose, const char* wild)
  {
    if (instance())
    {
      return (instance()->_showStatus(thd, verbose, wild));
    }
    else
    {
      my_message(ER_UNKNOWN_ERROR, "GoogleStats not initialized.", MYF(0));
      return -1;
    }
  }

  // See ::_setStatus for comments.
  static int setStatus()
  {
    /*
      Short circuit instance() call (which logs an error if uninitialized) with
      a check of _instance; this prevents spam in mysql.err if someone calls
      SHOW GLOBAL STATUS and GoogleStats is disabled.
    */
    if (_instance != 0 && instance())
      return (instance()->_setStatus());
    else
      return -1;
  }

  static bool createInstance();
  static bool destroyInstance();

private:

  StatsServerCache();
  ~StatsServerCache();

  static StatsServerCache* _instance;

  // Return the singleton.
  static StatsServerCache* instance() {
    if (_instance == 0) {
      sql_print_error("StatsServerCache: not initialized!");
      return 0;
    } else {
      return(_instance);
    }
  }

  // Describes one stats server.
  struct ServerInfo {
    std::string name;        // hostname for the stats server
    struct sockaddr_in addr; // IP address for the stats server
    time_t lastAccessed;     // time when last used
    int port;                // port for the stats server
    int connectTry;          // #connection attempts to the server
    int connectFail;         // #failed connection attempts to the server
    bool isAlive;            // true when the server might be alive

    ServerInfo(const char* n, int p)
        : name(n),
          lastAccessed(0),
          port(p),
          connectTry(0),
          connectFail(0),
          isAlive(true)
    { }

    ServerInfo()
        : lastAccessed(0),
          port(0),
          connectTry(0),
          connectFail(0),
          isAlive(false)
    { }

    int loadAddr(); // set addr
    int operator==(const ServerInfo& i) const {
      return (name == i.name && port == i.port);
    }
  };

  // All servers loaded from LocalStatsServers.
  typedef std::vector<ServerInfo*> ServerInfoPtrVector;
  ServerInfoPtrVector servers;

  // Describes one table at a stats server.
  struct TableAtServer {
    TableAtServer(ServerInfo* s, time_t a)
        : server(s), lastAccessed(a), isGood(true), fetchFail(0) { }
    ServerInfo* server;  // stats server for this table.
    time_t lastAccessed; // time when table last used
    bool isGood;         // true when this table might not be corrupt
    int fetchFail;       // #times fetch commands fail for this table
  };

  // Store (db.table, tier) or (db, tier) to be used as a key.
  typedef std::pair<std::string, int> TblMapKey;

  // Map (db.table, tier) to servers that provide that table.
  // Map (db, tier) to servers that provide all tables for a db.
  typedef std::map<TblMapKey, std::vector<TableAtServer> > TblMap;
  TblMap tblMap;

  // Stores values extracted from tblMap to be returned to the
  // client during SHOW STATS_SERVER STATUS.
  struct StatusValues {
    std::string dbTable;
    std::string host;
    int port;
    int tier;
    bool isAlive;
    time_t lastAccessed;
    bool isGood;
    int connectTry;
    int connectFail;
    int fetchFail;
  };

  void getStatusValues(std::vector<StatusValues>* values);

  // Clear the cache of all server and table data. mutex must be locked
  // when this is called.
  void clear();

  // Must be held when tblMap and servers are accessed.
  pthread_mutex_t mutex;

  // Return a connection to a stats server. The stats server is selected
  // randomly from the servers that are alive. The search begins at tier 0
  // unless 'state' has been initialized from a previous call.  Return 0 on
  // success, or an error code (through errorCode). mutex must not be locked
  // when this is called.
  // Parameters:
  //   dbName, tableName, indexName -
  //     get connection for dbname.tableName.indexName
  //   serverName - return hostname for the stats server
  //   port - return port for the stats server
  //   state - used to restart the search when called multiple times
  //   errorCode - Returns an error code or 0 on success.
  int _connect(const char* dbName,
               const char* tableName,
               const char* indexName,
               std::string* serverName,
               int* port,
               StatsServerConnectState* state,
               int* errorCode);

  // Query googlestats_servers_table (LocalStatsServers), clear and
  // repopulate the server cache (tblMap, servers). mutex must no be locked
  // when this is called. Calls ::load and then ::initCache. Return true on
  // success. Does not change instance state on failure.
  bool _reinit(THD* thd);

  // Write the status for all stats servers and stats server tables to
  // the user's network connection. The results are formatted to look
  // like a query. The results are denormalized (servers X tables) so
  // that server status is repeated for every table on that server. mutex
  // must not be locked when this is called.
  // If verbose is false, only hosts that have been accessed are returned.
  // Tables or host names can be filtered using the wild parameter.
  int _showStatus(THD* thd, bool verbose, const char* wild);

  // Set statsserver variables to show for SHOW STATUS. Caller must lock
  // LOCK_status.
  int _setStatus();

  // Stores one row from googlestats_servers_table
  struct ServerTblRecord {
    std::string serverName;
    std::string dbName;
    std::string tblName;
    int port;
    int tier;

    ServerTblRecord(const std::string& s,
                    const std::string& d,
                    const std::string& t,
                    int p,
                    int tr)
        : serverName(s),
          dbName(d),
          tblName(t),
          port(p),
          tier(tr)
    { }
  };
  typedef std::vector<ServerTblRecord> ServerRecordVector;

  // Scans googlestats_servers_table (LocalStatsServers) for the database dbName
  // and returns the results in serverRecords. Don't change instance state on
  // failure. Return 0 on success.
  int load(THD* thd, const char* dbName, ServerRecordVector* serverRecords);

  // Clear and then repopulate the table and server cache using the data from
  // serverRecords. mutex must not be locked when this is called. Do not change
  // instance state on error. Return true on success.
  bool initCache(const ServerRecordVector& serverRecords);

  // Scans serverRecords and adds entries to:
  //   wildcardServers for entries with tblName = ''
  //   byTableServers for entries with tblName = 'table'
  //   byIndexServers for entries with tblName = 'table.index'
  //
  // qualNameToParts maps a name to its parts and is updated for each new name.
  //   'db' -> ['db']
  //   'db.table' -> ['db', 'table']
  //   'db.table.index' -> ['db', 'table', 'index']
  //
  //  newServers returns an entry for each new stats server encountered.
  //
  // resultTblMap returns the values for StatsServerCache::tblMap if the cache
  // reload succeeds.
  //
  // Static to avoid updating instance fields.
  // Returns true on success. Regardless of result, caller owns memory in
  // newServers.
  static bool processServerRecords(
      const ServerRecordVector& serverRecords,
      ServerInfoPtrVector* newServers,
      std::map<std::string, std::vector<std::string> >* qualNameToParts,
      TblMap* wildcardServers,
      TblMap* byTableServers,
      TblMap* byIndexServers);

  // Clears servers and deletes all elements in it.
  static void clearServerRecords(ServerInfoPtrVector* servers);

  // Prepare entries to be added to tblMap by creating fake entries from
  // fromServers that match entries in toServers. fromServers has names using
  // the formats '' and 'table'. toServers has names using the formats 'table'
  // and 'db.table.index'. All names are for the same database, so the database
  // name is not included. When prefixLength=0, fromServers has names like ''
  // and fake entries are created for every server in toServers when the tiers
  // match. When prefixLength=1, fromServers has names like 'table', toServers
  // has names like 'table' or 'table.index' and fake entries are created for
  // every server in toServers when the tiers and table names match.
  //
  // See the class comment for StatsServerCache in gss_cache.h for details on
  // why this is done.
  //
  // See processServerRecords above for details on qualNameToParts.
  //
  // The fake entries are added to insertItems.
  // For example, assume an entry is (db,name,tier,server) where name is '',
  // 'table' or 'table.index' and
  //    prefixLength = 0
  //    fromServers = { (a,'',1,s1), (a,'',2,s2) }
  //    toServers = { (a,'foo',1,s3) }
  // then append to insertItems = { (a,'foo',1,s1) }
  // The entry added to insertItems is fake because such a row is not in
  // LocalStatsServers.
  //
  // Also assume:
  //    prefixLength = 1
  //    fromServers = { (a,'foo',1,s1), (a,'bar',2,s2) }
  //    toServers = { (a,'foo.idx',1,s3) }
  // then append to insertItems = { (a,'foo.idx',1,s1) }
  // The entry added to insertItems is fake because such a row is not in
  // LocalStatsServers.
  //
  // And the purpose behind this is to make it possible to find all servers
  // that can serve db.table.index when the key used to lookup the entries
  // is db.table or dt.table.index.
  //
  // Return 0 on success.
  // static to avoid updating instance fields.
  static int getFakeEntries(
      const TblMap& fromServers,
      const TblMap& toServers,
      const int prefixLength,
      const std::map<std::string, std::vector<std::string> >& qualNameToParts,
      std::vector<std::pair<TblMapKey, TableAtServer> >* insertItems);

  // Confirms that the schema for googlestats_servers_table (LocalStatsServers)
  // is correct.
  int checkSchema(TABLE* tbl);

  // Find a stats server that has data for a given table. Return 0 on success,
  // and an errorCode on some failure. mutex must not be locked when this is
  // called.
  // Parameters
  //   dbTable - the table name ('table')
  //   dbTableIndex - table and index name ('table.index')
  //   dbName - database name
  //   server - returns the stats server to use.
  //   state - describes where to restart the search when initialized
  //           from a previous call
  int findServer(const std::string& dbTable,
                 const std::string& dbTableIndex,
                 const std::string& dbName,
                 ServerInfo* server,
                 StatsServerConnectState* state);

  // Mark the server dead. This should be called after a connect attempt fails.
  // mutex must not be locked when this is called.
  void _markServerDead(const ServerInfo& server);

  // Mark table@server bad. This should be called after a FETCH command fails.
  // mutex must not be locked when this is called.
  // Parameters:
  //   dbName - database for the table
  //   tableName - unqualified table name
  //   indexName - index to be marked as bad
  //   serverName, port - host and port for the server
  void _markTableBadAtServer(const char* dbName,
                             const char* tableName,
                             const char* indexName,
                             const char* serverName,
                             int port);

  // Mark the server dead after a connect failure. mutex must not be locked
  // when this is called.
  // Parameters:
  //   errmsg - message for the error log
  //   server - describes the server for the failure
  //   sock_fd - set to -1
  void handleConnectFailure(const char* errmsg,
                            const ServerInfo& server,
                            int* sock_fd);

  // Helper method to mark any table bad that uses the server
  // described by serverInfo. mutex must be locked when this is called.
  void markBad(const ServerInfo& serverInfo,
               const std::string& qualifiedTblName,
               TblMap::iterator* mapEntry);

  // Helper method to add entry to 'tables'. mutex must be locked when
  // this is called. Static because it is called by static methods.
  static void addToTblMap(const std::string& qualifiedTblName,
                          int tier,
                          bool print,
                          ServerInfo* server,
                          TblMap* tables);

  // Helper method to add all entries from myMap to tblMap.
  void addAllEntries(const TblMap& fromMap);

  // Return the qualified name for a table to be used to access a stats server.
  //   dbTable returns the name as 'db.table'
  //   dbTableIndex returns the name as 'db.table.index' when indexName is
  //     not null and '' otherwise.
  //   tableName is processed by removing trailing '2's and a trailing date that
  //     can be used as aliases for tables during schema changes.
  // Returns 0 on success.
  int getQualifiedName(const char* dbName,
                       const char* tableName,
                       const char* indexName,
                       std::string* dbTable,
                       std::string* dbTableIndex);

  // Extracts the components of a dotted name ('a.b.c') and returns the number
  // of components. Returns a GsName value (GSN_ERR on failure). When GSN_DB
  // is returned, tableName and indexName return ''. When GSN_DB_TABLE is
  // returned, indexName returns ''. Otherwise, tableName and indexName return
  // non-empty strings.
  //   dbName is the database name
  //   tableNameFromRow is a value from LocalStatsServers.TableName. This
  //     is one of '', 'table', or 'table.index'
  //   qualifiedName returns the qualified name for the table, either 'db',
  //     'db.table' or 'db.table.index'
  //   tableName returns the table name extracted from tableNameFromRow or ''
  //   indexName returns the index name extracted from tableNameFromRow or ''
  enum GsName {
    GSN_DB,
    GSN_DB_TABLE,
    GSN_DB_TABLE_INDEX,
    GSN_ERR
  };
  static GsName classifyName(const std::string& dbName,
                             const std::string& tableNameFromRow,
                             std::string* qualifiedName,
                             std::string* tableName,
                             std::string* indexName);
};

#endif // GSS_CACHE_H
