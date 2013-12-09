// Copyright 2013 Google Inc. All Rights Reserved.

#include <map>
#include <set>
#include <vector>
#include <algorithm>
#include <string>
#include <cstdlib>

#include "my_global.h"
#include "mysqld.h"
#include <m_ctype.h>
#include "sql_base.h"
#include "lock.h"
#include "records.h"
#include "sql_acl.h"
#include "gss_cache.h"
#include "gss_aux.h"
#include "gss_errors.h"
#include "ha_googlestats.h"
#include "status_vars.h"
#include "sql_show.h"
#include "transaction.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/utsname.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>

StatsServerCache* StatsServerCache::_instance = 0;

bool googlestats_reinit(THD* thd) {
  // false means success to MySQL, so invert reality here
  return !StatsServerCache::reinit(thd);
}

int googlestats_show_status(THD* thd, bool verbose, const char* wild) {
  return StatsServerCache::showStatus(thd, verbose, wild);
}

int googlestats_set_status() {
  return StatsServerCache::setStatus();
}

StatsServerCache::StatsServerCache() {
  pthread_mutex_init(&mutex, MY_MUTEX_INIT_FAST);
}

StatsServerCache::~StatsServerCache() {
  pthread_mutex_lock(&mutex);
  clear();
  pthread_mutex_unlock(&mutex);
  pthread_mutex_destroy(&mutex);
}

bool
StatsServerCache::createInstance() {
  _instance = new StatsServerCache();
  if (_instance == 0) {
    StatsServerAux::printError("Failed to start GoogleStats");
    return false;
  } else {
    bool res = _instance->_reinit(0);
    if (!res) {
      StatsServerAux::printError("Failed to start GoogleStats");
      delete _instance;
      _instance = NULL;
      return false;
    } else {
      StatsServerAux::printError("GoogleStats started");
      return true;
    }
  }
}

bool
StatsServerCache::destroyInstance() {
  if (_instance) {
    delete _instance;
    _instance = NULL;
    StatsServerAux::printError("GoogleStats stopped");
  }
  return true;
}

#ifndef GETHOSTBYNAME_BUFF_SIZE
#define GETHOSTBYNAME_BUFF_SIZE 2048
#endif

int
StatsServerCache::ServerInfo::loadAddr() {
  if (name.c_str() == 0) {
    // how did this happen?
    StatsServerAux::printError("StatsServerCache: server name is NULL (port = %d)",
      port);
    return(-1);
  }

  struct hostent host;
  char buf[GETHOSTBYNAME_BUFF_SIZE];
  struct hostent *hostp =
      StatsServerAux::getHostByName(name.c_str(), &host, buf, sizeof(buf));
  if (!hostp) {
    return(-1);
  }

  bzero(&addr, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  memcpy(&addr.sin_addr, hostp->h_addr_list[0], hostp->h_length);
  return(0);
}

void
StatsServerCache::handleConnectFailure(const char* errmsg,
                                       const ServerInfo& server,
                                       int* sock_fd) {
  StatsServerAux::printError("StatsServerCache: couldn't connect to server "
                             "(%s:%d), errno(%d) : %s",
                             server.name.c_str(), server.port,
                             errno, errmsg);
  ::close(*sock_fd);
  *sock_fd = -1;
  _markServerDead(server);
  statistic_increment(google_connect_failures, &LOCK_stats);
}

int StatsServerCache::getQualifiedName(const char* dbName,
                                       const char* tableName,
                                       const char* indexName,
                                       std::string* dbTable,
                                       std::string* dbTableIndex) {
  const char* kWho = "StatsServerCache::getQualifiedName";
  dbTable->clear();
  dbTableIndex->clear();

  if (!dbName || strlen(dbName) == 0) {
    StatsServerAux::printError("%s: dbName is empty", kWho);
    return -1;
  }
  if (!tableName || strlen(tableName) == 0) {
    StatsServerAux::printError("%s: tableName is empty", kWho);
    return -1;
  }

  int len = StatsServerAux::RemoveTrailing2sAndDate(tableName);
  if (len <= 0) {
    StatsServerAux::printError("%s: Cannot format db(%s) and table(%s)",
                               kWho, dbName, tableName);
    return -1;
  }
  *dbTable = std::string(dbName) + "." +
             std::string(tableName, tableName + len);
  if (indexName)
    *dbTableIndex = *dbTable + "." + indexName;

  return 0;
}

StatsServerCache::GsName
StatsServerCache::classifyName(const std::string& dbName,
                               const std::string& tableNameFromRow,
                               std::string* qualifiedName,
                               std::string* tableName,
                               std::string* indexName) {
  const char* kWho = "classifyName";
  qualifiedName->clear();
  tableName->clear();
  indexName->clear();

  if (dbName.empty()) {
    StatsServerAux::printError("%s: dbName is empty", kWho);
    return GSN_ERR;
  }

  if (tableNameFromRow.empty()) {
    // numDots = 0, db name is valid
    *qualifiedName = dbName;
    return GSN_DB;
  }

  *qualifiedName = dbName + "." + tableNameFromRow;

  std::string::size_type firstDot = tableNameFromRow.find('.', 0);
  if (firstDot == std::string::npos) {
    // numDots = 1, db and table name are valid
    *tableName = tableNameFromRow;
    return GSN_DB_TABLE;
  }

  if ((firstDot + 1) == tableNameFromRow.size()) {
    StatsServerAux::printError("%s: table name %s ends with \'.\'",
                               kWho, tableNameFromRow.c_str());
    return GSN_ERR;
  }

  if (firstDot == 0) {
    StatsServerAux::printError("%s: table name %s starts with \'.\'",
                               kWho, tableNameFromRow.c_str());
    return GSN_ERR;
  }

  // This is either table.index or table.index.bad and only one '.'
  // should be in the name.
  if (tableNameFromRow.find('.', firstDot + 1) != std::string::npos) {
    StatsServerAux::printError("%s: table name %s has too many \'.\'",
                               kWho, tableNameFromRow.c_str());
    return GSN_ERR;
  }

  // Return 'table' from 'table.index'
  *tableName = tableNameFromRow.substr(0, firstDot);

  // Return 'index' from 'table.index'
  *indexName = tableNameFromRow.substr(firstDot + 1);

  return GSN_DB_TABLE_INDEX;
}

// Increment a global counter to track the tiers at which stats server
// connections are made.
static void
incrementConnectAtTier(int tier) {
  static ulong* counters[] = {
    &google_connect_tier0,
    &google_connect_tier1,
    &google_connect_tier2,
    &google_connect_tier3,
    &google_connect_tier4,
    &google_connect_tier5,
    &google_connect_tier6,
    &google_connect_tier7,
    &google_connect_tier8,
    &google_connect_tier9
  };

  if (tier < 0 || tier > 9) {
    StatsServerAux::printError("StatsServerCache: "
                               "Bad tier(%d) for connect counter", tier);
  } else {
    statistic_increment(*counters[tier], &LOCK_stats);
  }
}

int
StatsServerCache::_connect(const char* dbName,
                           const char* tableName,
                           const char* indexName,
                           std::string* serverName,
                           int* port,
                           StatsServerConnectState* state,
                           int* errorCode) {
  const char* kWho = "StatsServerCache::_connect";
  std::string dbTable;
  std::string dbTableIndex;
  *errorCode = 0;

  if (!indexName) {
    StatsServerAux::printError("%s: index name not provided for %s",
                               kWho, dbTable.c_str());
    DBUG_ASSERT(indexName);
    *errorCode = GSS_ERR_BAD_INDEX_NAME;
    return -1;
  }
  if (getQualifiedName(dbName, tableName, indexName, &dbTable, &dbTableIndex)) {
    StatsServerAux::printError("%s: cannot make a table name", kWho);
    *errorCode = GSS_ERR_BAD_INDEX_NAME;
    return -1;
  }
  DBUG_ASSERT(dbTable.size());
  DBUG_ASSERT(dbTableIndex.size());

  if (state->getIsDone()) {
    StatsServerAux::printError("%s: connection state is done", kWho);
    *errorCode = GSS_ERR_CONNECT_SERVER;
    return -1;
  }

 retry:
  int flags;
  int sock_fd = -1;
  int res = 0;

  while(1) {
    ServerInfo server;
    if ((res = findServer(dbTable, dbTableIndex, dbName, &server, state))) {
      StatsServerAux::printError("%s: couldn't find server for %s",
                                 kWho, dbTableIndex.c_str());
      *errorCode = res;
      goto cleanup;
    }
    *serverName = server.name;
    *port = server.port;

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
      StatsServerAux::printError("%s: couldn't create socket", kWho);
      *errorCode = GSS_ERR_SOCKET_CREATE;
      goto cleanup;
    }

    // set socket temporarily to non-blocking
    flags = fcntl(sock_fd, F_GETFL);
    fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);

    res = ::connect(sock_fd, (struct sockaddr *) &server.addr,
                    sizeof(server.addr));
    if (res == 0) {
      // connected right away, we're done
      incrementConnectAtTier(state->tier);
      goto cleanup;
    }
    if (errno != EINPROGRESS) {
      // Connect failed, close the socket and try another server.
      handleConnectFailure("unexpected errno", server, &sock_fd);
      StatsServerAux::printError("errno %d: %s", errno, strerror(errno));
      *errorCode = GSS_ERR_SOCKET_CREATE;
      goto cleanup;
    }

    // Wait for the connect to complete.
    bool timedOut;
    res = StatsServerAux::waitForSocket(sock_fd, serverName->c_str(),
                                        googlestats_timeout, true, &timedOut);
    if (res < 0) {
      // Timeout or error. Close socket and try again.
      handleConnectFailure(timedOut ? "timed out" : "other error", server,
                           &sock_fd);
      continue;
    } else {
      // The connection is good.
      incrementConnectAtTier(state->tier);
      goto cleanup;
    }
  }
  // Fell through without a connection.
  DBUG_ASSERT(sock_fd == -1);

cleanup:
  if (sock_fd != -1) {
    struct linger linger_opt = {1, 2};  // linger on, linger time 2s
    if (setsockopt(sock_fd, SOL_SOCKET, SO_LINGER, &linger_opt,
                   sizeof(linger_opt))) {
      StatsServerAux::printError("%s: linger setting error(%d)", kWho, errno);
    }

    int on = 1;
    if (setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on))) {
      StatsServerAux::printError("%s: no_delay setting error(%d)", kWho, errno);
    }

    if (googlestats_log_level >= GsLogLevelMed &&
        (fcntl(sock_fd, F_GETFL) & O_NONBLOCK) != 0) {
      StatsServerAux::printError("%s: nonblocking socket!", kWho);
    }
    return(sock_fd);
  } else if (!state->getHasRetried()) {
    // If an alive server and good table cannot be found, restart the search at tier 0
    // and try all servers regardless of the server and table state.
    state->setHasRetried();
    goto retry;
  } else {
    DBUG_ASSERT(sock_fd == -1);
    statistic_increment(google_connect_not_possible, &LOCK_stats);
    if (*errorCode == 0) *errorCode = GSS_ERR_CONNECT_SERVER;
    return -1;
  }
}


void
StatsServerCache::clearServerRecords(ServerInfoPtrVector *servers) {
  ServerInfoPtrVector::iterator server;
  for (server = servers->begin(); server != servers->end(); ++server) {
    ServerInfo* s = *server;
    delete s;
  }
  servers->clear();
}

void
StatsServerCache::clear() {
  clearServerRecords(&servers);
  tblMap.clear();
}

// check schema of config table
int StatsServerCache::checkSchema(TABLE* tbl) {
  if (tbl->s->keys == 0) {
    StatsServerAux::printError("StatsServerCache: config table %s.%s does not "
                               "have primary key", tbl->s->db.str,
                               tbl->s->table_name.str);
    return(-1);
  }
  KEY* key_info = &tbl->key_info[tbl->s->primary_key];

  if (strcmp(key_info->name, "PRIMARY") != 0) {
    StatsServerAux::printError("StatsServerCache: not scanning primary index "
                               "of config table %s.%s", tbl->s->db.str,
                               tbl->s->table_name.str);
    return(-1);
  }
  if (tbl->s->fields != 4) {
    StatsServerAux::printError("StatsServerCache: config table %s.%s doesn't "
                               "have 4 columns", tbl->s->db.str,
                               tbl->s->table_name.str);
    return(-1);
  }

  // make sure first column is 'Host varchar(128) not null'
  // The varchar format in 5.0 is len:data with sizeof(len) = 1
  Field* field = tbl->field[0];
  if (strcmp(field->field_name, "Host") != 0 ||
      field->type() != MYSQL_TYPE_VARCHAR ||
      field->pack_length() != 129 ||
      field->maybe_null())
  {
    StatsServerAux::printError("StatsServerCache: first column of config "
                               "table %s.%s must be "
                               "'Host varchar(128) not null'", tbl->s->db.str,
                               tbl->s->table_name.str);
    return(-1);
  }

  // make sure second column is 'Port int not null'
  field = tbl->field[1];
  if (strcmp(field->field_name, "Port") != 0 ||
      field->type() != MYSQL_TYPE_LONG ||
      field->maybe_null())
  {
    StatsServerAux::printError("StatsServerCache: second column of config "
                               "table %s.%s must be 'Port int not null'",
                               tbl->s->db.str, tbl->s->table_name.str);
    return(-1);
  }

  // make sure third column is 'TableName varchar(128) not null'
  // The varchar format in 5.0 is len:data with sizeof(len) = 1
  field = tbl->field[2];
  if (strcmp(field->field_name, "TableName") != 0 ||
      field->type() != MYSQL_TYPE_VARCHAR ||
      field->pack_length() != 129 ||
      field->maybe_null())
  {
    StatsServerAux::printError("StatsServerCache: third column of config "
                               "table %s.%s must be 'TableName varchar(128) "
                               "not null'", tbl->s->db.str, tbl->s->table_name.str);
    return(-1);
  }

  // make sure fourth column is 'Tier int not null'
  field = tbl->field[3];
  if (strcmp(field->field_name, "Tier") != 0 ||
      field->type() != MYSQL_TYPE_LONG ||
      field->maybe_null())
  {
    StatsServerAux::printError("StatsServerCache: fourth column of config "
                               "table %s.%s must be 'Tier int not null'",
                               tbl->s->db.str, tbl->s->table_name.str);
    return(-1);
  }

  return(0);
}

bool
StatsServerCache::_reinit(THD* orig_thd) {
  // ugh -- this must be first so that thread_stack can be determined below.
  // This is very ugly and error prone and appears to be required. The
  // start of the thread stack is the address of the first local variable
  // in this function.
  THD* thd;

  // Re-seed random number generator, we're using this for stochastic load
  // balancing in findServer(). Use time() as seed to avoid all databases
  // hitting stats servers in same order.
  const char* kWho = "StatsServerCache::_reinit";
  int seed = 1;
  time_t time_val;
  time(&time_val);
  memcpy(&seed, &time_val, sizeof(seed));
  std::srand(seed);
  bool result = false;

  // to be able to run this from boot, we allocate a temporary THD
  if ((thd = new THD) == 0) {
    StatsServerAux::printError("%s: can't allocate THD", kWho);
    return false;
  }
  thd->thread_stack = (char*) &thd;
  thd->store_globals();
  thd->db = my_strdup("mysql", MYF(0));
  if (!thd->db) {
    StatsServerAux::printError("%s: can't strdup db name", kWho);
    delete thd;
    return false;
  }
  thd->db_length = 5;

  // declare before 'goto' to avoid compiler complaints
  ServerRecordVector serverRecords;
  char error_text[100] = { '\0' };

  // Load stats server info for all databases that have a config table.
  // This piece of code is lifted from sql_show.cc. If in the future
  // the mysql guys give us a function that'll just give us a list of db names
  // back, use that one.
  Dynamic_array<LEX_STRING*> db_names;
  LEX_STRING db_name_wild= { NULL, 0 };
  if (find_files(thd, &db_names, NULL, mysql_data_home, &db_name_wild)) {
    StatsServerAux::printError("%s: couldn't load database names", kWho);
    sprintf(error_text, "couldn't load database names");
    goto cleanup;
  }

  for(size_t i= 0; i < db_names.elements(); i++) {
    LEX_STRING *db_name = db_names.at(i);
    if (load(thd, db_name->str, &serverRecords)) {
      StatsServerAux::printError("%s: cannot load %s.LocalStatsServers", kWho,
                                 db_name->str);
      sprintf(error_text, "cannot load %s.LocalStatsServers", db_name->str);
      goto cleanup;
    }
  }

  result = true;

cleanup:
  trans_commit_stmt(thd);
  trans_commit(thd);
  close_thread_tables(thd);
  delete thd;
  if (orig_thd != 0) {
    orig_thd->store_globals();
  } else {
    // remember that we don't have a THD
    my_pthread_setspecific_ptr(THR_THD,  0);
  }

  if (result) {
    // get mutex only after we got rid of all mysql resources
    return initCache(serverRecords);
  } else {
    // there was already a failure, don't try to load the cache.
    my_message(ER_UNKNOWN_ERROR, error_text, MYF(0));
    return false;
  }
}

int
StatsServerCache::load(THD* thd,
                       const char* dbName,
                       ServerRecordVector* serverRecords) {
  const char* kWho = "StatsServerCache::load";
  int result = 0;

  if (googlestats_log_level >= GsLogLevelMed) {
    StatsServerAux::printError("%s: loading %s for database %s",
                               kWho, googlestats_servers_table, dbName);
  }

  // open table to start scan of stats servers config table
  if (googlestats_servers_table == 0) {
    StatsServerAux::printError("%s: googlestats_servers_table not set", kWho);
    return -1;
  }

  TABLE* tbl;
  MYSQL_LOCK* lock;
  READ_RECORD read_record_info;

  // save proc_info, open_tables() sets it to 0
  const char* proc_info = current_thd->proc_info;

  TABLE_LIST table_list;
  table_list.init_one_table(dbName, strlen(dbName),
                            googlestats_servers_table,
                            strlen(googlestats_servers_table),
                            googlestats_servers_table,
                            TL_READ);
  TABLE_LIST* table_list_ptr = &table_list;

  // New in 5.1; partitioning requires lexing to be started. This should not
  // affect us for reading from LocalStatsServers.
  lex_start(thd);

  uint ctr = 0;

  if (open_tables(thd, &table_list_ptr, &ctr, 0)) {
    // This is an acceptable failure.
    if (googlestats_log_level >= GsLogLevelMed)
      StatsServerAux::printError("%s: open_tables failed for %s", kWho, dbName);
    return 0;
  }

  tbl = table_list.table;
  if ((lock = mysql_lock_tables(thd, &tbl, 1, 0)) == 0)
  {
    StatsServerAux::printError("%s: can't lock table %s.%s", kWho,
                               dbName, googlestats_servers_table);
    return -1;
  }

  // TODO(mcallaghan): Is this OK? Should it use the macro?
  // Restore proc_info to its previous value.
  current_thd->proc_info = proc_info;

  // Set bitmap of all of the fields in this table, otherwise the scan
  // won't pick them up.
  for (uint i = 0; i < tbl->s->fields; ++i) {
    bitmap_set_bit(tbl->read_set, tbl->field[i]->field_index);
  }

  if (checkSchema(tbl)) {
    // Doesn't look like we got the right table.
    StatsServerAux::printError("%s: bad schema for %s.%s", kWho,
                               dbName, googlestats_servers_table);
    result = -1;
    goto cleanup;
  }

  // TODO: Do we need this?
  tbl->file->init_table_handle_for_HANDLER();

  init_read_record(&read_record_info, thd, tbl,
                   NULL,
                   0, true, false);
  while (!(read_record_info.read_record(&read_record_info))) {
    std::string tblName, serverName, portStr, tierStr;
    StatsServerAux::printColVal(tbl->field[0], &serverName);
    StatsServerAux::printColVal(tbl->field[1], &portStr);
    StatsServerAux::printColVal(tbl->field[2], &tblName);
    StatsServerAux::printColVal(tbl->field[3], &tierStr);
    int port, tier;
    port = atoi(portStr.c_str());
    tier = atoi(tierStr.c_str());
    serverRecords->push_back(ServerTblRecord(serverName, dbName, tblName,
                                             port, tier));
  }
  end_read_record(&read_record_info);

  // TODO: Do we really need this?
  tbl->file->init_table_handle_for_HANDLER();

cleanup:
  mysql_unlock_tables(thd, lock);
  return result;
}


int StatsServerCache::findServer(const std::string& dbTable,
                                 const std::string& dbTableIndex,
                                 const std::string& dbName,
                                 ServerInfo* returnServer,
                                 StatsServerConnectState* state) {
  const char* kWho = "StatsServerCache::findServer";
  DBUG_ASSERT(!state->getIsDone());

  if (state->searchTier > kMaxStatsServerTier || state->searchTier < 0) {
    StatsServerAux::printError("%s: no more servers to try for %s",
                               kWho, dbTableIndex.c_str());
    return GSS_ERR_CONNECT_SERVER;
  }

  (void) pthread_mutex_lock(&mutex);

  // Start with the tier where the previous search ended if possible.
  for (; state->searchTier <= kMaxStatsServerTier; ++state->searchTier) {
    TblMap::iterator mapEntry;

    // Start with servers sharded by index
    mapEntry = tblMap.find(std::make_pair(dbTableIndex, state->searchTier));

    // Try servers sharded by table only if there are no servers sharded by
    // index. If there were, then fake entries would have been created for
    // the servers sharded by index and they would have been found above.
    if (mapEntry == tblMap.end())
      mapEntry = tblMap.find(std::make_pair(dbTable, state->searchTier));

    if (mapEntry == tblMap.end()) {
      // Look for a wildcard server only when there are no non-wildcard entries.
      // If there are non-wildcard entries, then 'fake' entries for the
      // wildcard server have been added to all tblMap entries for the given
      // dbName.
      mapEntry = tblMap.find(std::make_pair(dbName, state->searchTier));
      if (mapEntry == tblMap.end()) {
        state->currentOffset = -1;  // value was valid for the previous tier
        continue;
      }
    }

    // Start where the previous search ended if possible.
    int numEntries = mapEntry->second.size();
    DBUG_ASSERT(numEntries != 0);

    // At least one server that has this table was found.
    state->foundMatch = true;

    // Start at the offset where the previous search ended. The >= check is done
    // because the cache could be flushed between calls to findServer.
    if (state->currentOffset == -1 || state->currentOffset >= numEntries) {
      state->startIndex = std::rand() % numEntries;
      state->currentOffset = 0;
    }

    // Look for an alive server and good table if this is the first pass through
    // the set of servers, otherwise try all servers that have the table.
    for (; state->currentOffset < numEntries; ++state->currentOffset) {
      int idx = (state->currentOffset + state->startIndex) % numEntries;
      time_t now = ::time(0);
      if (now <= 0)
        StatsServerAux::printError("%s: time returns %d", kWho, (int) now);

      TableAtServer& tableAtServer = mapEntry->second[idx];
      ServerInfo* useServer = tableAtServer.server;

      // Reset the state of the dead servers and bad tables if more than
      // googlestats_retry_interval seconds have passed.
      if (!useServer->isAlive &&
          (now - useServer->lastAccessed) > googlestats_retry_interval) {
        useServer->isAlive = true; // We'll find out soon.
      }
      if (!tableAtServer.isGood &&
          (now - tableAtServer.lastAccessed) > googlestats_retry_interval) {
        tableAtServer.isGood = true; // Try to access this table again.
      }

      // Try this table@server if this is the second pass or if the server
      // is alive and the table is good.
      if (state->hasRetried ||
          (useServer->isAlive && tableAtServer.isGood)) {
        if (useServer->isAlive && tableAtServer.isGood) {
          // Only update the lastAccessed time when it is expected that the
          // table@server is good and the server is alive. Avoid the
          // complication of updating the access time otherwise. Note that the
          // returned table@server might be marked as bad and the referenced
          // server as dead. If they turn out to be good and alive, their state
          // will be reset when the googlestats_retry_interval has passed or
          // flush stats_servers is executed.
          useServer->lastAccessed = now;
          tableAtServer.lastAccessed = now;
        }
        *returnServer = *useServer;
        useServer->connectTry++;
        state->tier = state->searchTier;

        // Start at the next server on this tier on the next search.
        ++state->currentOffset;

        if (state->currentOffset == numEntries) {
          // Start at the next tier on the next search because all servers
          // on this tier have been tried.
          state->currentOffset = -1;
          ++state->searchTier;
        }

        pthread_mutex_unlock(&mutex);
        return 0;
      }
    }
  }

  state->tier = state->searchTier;
  pthread_mutex_unlock(&mutex);
  return state->foundMatch ? GSS_ERR_CONNECT_SERVER : GSS_ERR_NO_SERVER;
}

void
StatsServerCache::markBad(const ServerInfo& serverInfo,
                          const std::string& qualifiedTblName,
                          TblMap::iterator* mapEntry) {
  std::vector<TableAtServer>& tables = (*mapEntry)->second;
  std::vector<TableAtServer>::iterator tableIter;
  for (tableIter = tables.begin(); tableIter != tables.end(); ++tableIter) {
    if (*(tableIter->server) == serverInfo) {
      tableIter->isGood = false;  // Prevent use of this entry.
      tableIter->fetchFail++;
      if (googlestats_log_level >= GsLogLevelMed) {
        StatsServerAux::printError("StatsServerCache: marked %s dead at %s:%d",
                                   qualifiedTblName.c_str(),
                                   serverInfo.name.c_str(), serverInfo.port);
      }
    }
  }
}

void
StatsServerCache::_markTableBadAtServer(const char* dbName,
                                        const char* tableName,
                                        const char* indexName,
                                        const char* serverName,
                                        int port) {
  const char* kWho = "StatsServerCache::_markTableBadAtServer";
  std::string dbTable;
  std::string dbTableIndex;
  if (getQualifiedName(dbName, tableName, indexName, &dbTable, &dbTableIndex)) {
    StatsServerAux::printError("%s: failed to get qualified name", kWho);
    return;
  }
  if (googlestats_log_level >= GsLogLevelMed) {
    StatsServerAux::printError("%s: mark bad %s for %s at %s:%d", kWho,
                               dbTable.c_str(), dbTableIndex.c_str(),
                               serverName, port);
  }

  ServerInfo serverInfo(serverName, port);
  (void) pthread_mutex_lock(&mutex);

  bool foundByIndex = false;

  // Mark matching servers for 'db.table.index'
  if (dbTableIndex.size()) {
    for (int tier = 0; tier <= kMaxStatsServerTier; ++tier) {
      TblMap::iterator mapEntry = tblMap.find(std::make_pair(dbTableIndex, tier));
      if (mapEntry != tblMap.end()) {
        foundByIndex = true;
        markBad(serverInfo, dbTableIndex, &mapEntry);
      }
    }
  }

  // Mark matching servers for 'db.table'. Only do this when entries were not
  // found using dbTableIndex because if any servers shard by index, then
  // fake entries are created for servers that shard by table and all would
  // be found by the code above.
  if (!foundByIndex) {
    for (int tier = 0; tier <= kMaxStatsServerTier; ++tier) {
      TblMap::iterator mapEntry = tblMap.find(std::make_pair(dbTable, tier));
      if (mapEntry != tblMap.end())
        markBad(serverInfo, dbTable, &mapEntry);
    }
  }

  // Mark matching wildcard entries dead. This matches the old behavior for
  // wildcard servers -- they are marked dead (for all tables) on any FETCH
  // failures.
  std::string dbnameString = dbName;
  for (int tier = 0; tier <= kMaxStatsServerTier; ++tier) {
    TblMap::iterator mapEntry = tblMap.find(std::make_pair(dbnameString, tier));
    if (mapEntry != tblMap.end())
      markBad(serverInfo, dbnameString, &mapEntry);
  }
  pthread_mutex_unlock(&mutex);
}

void
StatsServerCache::_markServerDead(const ServerInfo& serverInfo) {
  const char* kWho = "StatsServerCache::_markServerDead";
  if (googlestats_log_level >= GsLogLevelMed) {
    StatsServerAux::printError("%s: %s:%d", kWho, serverInfo.name.c_str(),
                               serverInfo.port);
  }

  (void) pthread_mutex_lock(&mutex);

  // Find the entry for this server. The entry might have changed since we got
  // a copy of it, but the server name and port won't have.
  ServerInfoPtrVector::iterator serverIter = servers.begin();
  for (; serverIter != servers.end(); ++serverIter) {
    if (serverInfo == **serverIter) {
      if ((*serverIter)->isAlive) {
        (*serverIter)->isAlive = false;
        (*serverIter)->connectFail++;
        if (googlestats_log_level >= GsLogLevelMed) {
          StatsServerAux::printError("%s: marked %s:%d dead", kWho,
                                     serverInfo.name.c_str(),
                                     serverInfo.port);
        }
      }
    }
  }
  pthread_mutex_unlock(&mutex);
}

void
StatsServerCache::getStatusValues(std::vector<StatusValues>* values) {
  // Get lock on internal data.
  (void) pthread_mutex_lock(&mutex);

  TblMap::iterator mapEntry;
  for (mapEntry = tblMap.begin(); mapEntry != tblMap.end(); ++mapEntry) {
    int tier = mapEntry->first.second;
    std::vector<TableAtServer>::iterator tableAtIter;
    for (tableAtIter = mapEntry->second.begin();
         tableAtIter != mapEntry->second.end();
         ++tableAtIter) {
      ServerInfo* server = tableAtIter->server;

      StatusValues value;
      value.dbTable = mapEntry->first.first;
      value.host = server->name;
      value.port = server->port;
      value.tier = tier;
      value.isAlive = server->isAlive;
      value.lastAccessed = tableAtIter->lastAccessed;
      value.isGood = tableAtIter->isGood;
      value.connectTry = server->connectTry;
      value.connectFail = server->connectFail;
      value.fetchFail = tableAtIter->fetchFail;

      values->push_back(value);
    }
  }

  pthread_mutex_unlock(&mutex);
}

// TODO(mcallaghan): Confirm that this does not leak memory.
int
StatsServerCache::_showStatus(THD* thd, bool verbose, const char* wild) {
  int status = 0;
  Protocol* protocol = thd->protocol;
  const char* kWho = "StatsServerCache::_showStatus";

  // Copy values to be returned from tblMap to avoid holding the mutex while
  // writing to the client and possibly blocking.
  std::vector<StatusValues> values;
  getStatusValues(&values);

  List<Item> field_list;
  std::vector<StatusValues>::const_iterator value_iter;

  Item *item_p[10];
  item_p[0] = new Item_empty_string("Database.Table", 20);
  item_p[1] = new Item_empty_string("Host", 20);
  item_p[2] = new Item_return_int("Port", 10, MYSQL_TYPE_LONG);
  item_p[3] = new Item_return_int("Tier", 10, MYSQL_TYPE_LONG);
  item_p[4] = new Item_empty_string("IsAlive", 5);
  item_p[5] = new Item_empty_string("LastAccessed", 26);
  item_p[6] = new Item_empty_string("TableGood", 5);
  item_p[7] = new Item_return_int("ConnectTry", 10, MYSQL_TYPE_LONG);
  item_p[8] = new Item_return_int("ConnectFail", 10, MYSQL_TYPE_LONG);
  item_p[9] = new Item_return_int("FetchFail", 10, MYSQL_TYPE_LONG);
  for (int idx = 0; idx < 10; ++idx) {
    if (item_p[idx] == NULL) {
      StatsServerAux::printError("StatsServerCache: error in send_fields()");
      status = -1;
      goto cleanup;
    }
    field_list.push_back(item_p[idx]);
  }

  if (protocol->send_result_set_metadata(
          &field_list, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
  {
    StatsServerAux::printError("%s: error in send_fields", kWho);
    status = -1;
    goto cleanup;
  }

  for (value_iter = values.begin(); value_iter != values.end(); ++value_iter) {
    // Skip row if it has a LIKE clause that doesn't match the table name or
    // host name.
    if (wild && wild[0] &&
        wild_case_compare(system_charset_info, value_iter->dbTable.c_str(),
                          wild) &&
        wild_case_compare(system_charset_info, value_iter->host.c_str(),
                          wild)) {
      continue;
    }

    // Skip row if it is SHOW CHANGED and the row has no non-default data.
    // Not including server-level values connectTry, connectFail, isAlive,
    // because then we get a row for every table on every server that has
    // ever been connected to.
    if (!verbose &&
        value_iter->lastAccessed == 0 &&
        value_iter->isGood &&
        value_iter->fetchFail == 0) {
      continue;
    }

    protocol->prepare_for_resend();

    if (protocol->store(value_iter->dbTable.c_str(), system_charset_info)) {
      StatsServerAux::printError("%s: write dbTable", kWho);
      status = -1;
      goto cleanup;
    }
    if (protocol->store(value_iter->host.c_str(), system_charset_info)) {
      StatsServerAux::printError("%s: write host", kWho);
      status = -1;
      goto cleanup;
    }
    if (protocol->store((uint)value_iter->port)) {
      StatsServerAux::printError("%s: write port", kWho);
      status = -1;
      goto cleanup;
    }
    if (protocol->store((uint)value_iter->tier)) {
      StatsServerAux::printError("%s: write tier", kWho);
      status = -1;
      goto cleanup;
      }

    const char* alive = value_iter->isAlive ? "true" : "false";
    if (protocol->store(alive, system_charset_info)) {
      StatsServerAux::printError("%s: write isAlive", kWho);
      status = -1;
      goto cleanup;
    }

    struct tm lastTime;
    localtime_r(&value_iter->lastAccessed, &lastTime);
    char lastAccessed[32];
    sprintf(lastAccessed, "%04d-%02d-%02d %02d:%02d:%02d",
            lastTime.tm_year + 1900, lastTime.tm_mon + 1,
            lastTime.tm_mday, lastTime.tm_hour, lastTime.tm_min,
            lastTime.tm_sec);
    if (protocol->store(lastAccessed, system_charset_info)) {
      StatsServerAux::printError("%s: write lastAccessed", kWho);
      status = -1;
      goto cleanup;
    }

    const char* good = value_iter->isGood ? "true" : "false";
    if (protocol->store(good, system_charset_info)) {
      StatsServerAux::printError("%s: write isGood", kWho);
      status = -1;
      goto cleanup;
    }

    if (protocol->store((uint) value_iter->connectTry)) {
      StatsServerAux::printError("%s: write connectTry", kWho);
      status = -1;
      goto cleanup;
    }
    if (protocol->store((uint) value_iter->connectFail)) {
      StatsServerAux::printError("%s: write connectFail", kWho);
      status = -1;
      goto cleanup;
    }
    if (protocol->store((uint) value_iter->fetchFail)) {
      StatsServerAux::printError("%s: write fetchFail", kWho);
      status = -1;
      goto cleanup;
    }

    if (protocol->write()) {
      StatsServerAux::printError("%s: error in row write", kWho);
      status = -1;
      goto cleanup;
    }
  }

  my_eof(thd);

cleanup:
  // TODO(mcallaghan): Is this needed?
  protocol->free();
  return(status);
}

int
StatsServerCache::_setStatus() {
  static ulong* counters[] = {
    &google_statsservers_tier0,
    &google_statsservers_tier1,
    &google_statsservers_tier2,
    &google_statsservers_tier3,
    &google_statsservers_tier4,
    &google_statsservers_tier5,
    &google_statsservers_tier6,
    &google_statsservers_tier7,
    &google_statsservers_tier8,
    &google_statsservers_tier9
  };

  // Use temporary counters for incrementing.
  ulong temp_counters[10];
  for (int i = 0; i <= 9; ++i) {
    temp_counters[i] = 0;
  }

  std::vector<StatusValues> values;
  std::vector<StatusValues>::const_iterator value_iter;
  getStatusValues(&values);

  // Count all the statsservers at each tier which are "alive".
  for (value_iter = values.begin(); value_iter != values.end(); ++value_iter) {
    if (value_iter->isAlive) {
      int tier = value_iter->tier;
      if (tier < 0 || tier > 9) {
        StatsServerAux::printError("StatsServerCache: "
                                   "Bad tier(%d) for statsserver counter",
                                   tier);
      } else {
        temp_counters[tier]++;
      }
    }
  }

  // Update the real, global counters. We assume the caller has locked
  // LOCK_status to make these updates consistent.
  mysql_mutex_assert_owner(&LOCK_status);
  for (int i = 0; i <= 9; ++i) {
    *(counters[i]) = temp_counters[i];
  }

  return 0;
}


void
StatsServerCache::addToTblMap(const std::string& qualifiedTblName,
                              int tier,
                              bool print,
                              ServerInfo* server,
                              TblMap* tables) {
  TblMapKey tblMapKey(qualifiedTblName, tier);
  TblMap::iterator mapEntry = tables->find(tblMapKey);
  // There is no entry. Add an empty vector to the map for the key.
  if (mapEntry == tables->end()) {
    mapEntry = tables->insert(
        std::make_pair(tblMapKey, std::vector<TableAtServer>())).first;
  }
  TableAtServer tableAtServer(server, 0);
  mapEntry->second.push_back(tableAtServer);
  if (print && googlestats_log_level >= GsLogLevelMed) {
    StatsServerAux::printError("StatsServerCache: add server for %s at %s:%d "
                               "to tier %d",
                               qualifiedTblName.c_str(), server->name.c_str(),
                               server->port, tier);
  }
}

bool
StatsServerCache::processServerRecords(
    const ServerRecordVector& serverRecords,
    ServerInfoPtrVector* newServers,
    std::map<std::string, std::vector<std::string> >* qualNameToParts,
    TblMap* wildcardServers,
    TblMap* byTableServers,
    TblMap* byIndexServers) {
  //
  // This must not update instance state until it is guaranteed to not fail.
  //

  // Hostnames that fail DNS lookups.
  std::set<std::pair<std::string, int> > badServers;

  wildcardServers->clear();
  byTableServers->clear();
  byIndexServers->clear();

  ServerRecordVector::const_iterator record;
  for (record = serverRecords.begin(); record != serverRecords.end(); ++record)
  {
    std::pair<std::string, int> badServerKey(record->serverName, record->port);
    if (badServers.find(badServerKey) != badServers.end()) {
      // DNS lookup for server:port failed on a previous iteration.
      continue;
    }

    // Look for this server.
    ServerInfo newServer(record->serverName.c_str(), record->port);
    ServerInfoPtrVector::iterator server;
    for (server = newServers->begin(); server != newServers->end(); ++server) {
      if (**server == newServer)
        break;
    }

    if (server == newServers->end()) {
      // Do not have an entry for this server, try to add one.
      ServerInfo* allocServer = new ServerInfo(record->serverName.c_str(),
                                               record->port);
      if (allocServer == NULL) {  // Could not get memory.
        StatsServerAux::printError("StatsServerCache: could not new server"
                                   "(%s:%d) for insufficient memory",
                                   record->serverName.c_str(), record->port);
        // Caller must free entries from newServers.
        return false;
      }
      if (googlestats_log_level >= GsLogLevelMed) {
        StatsServerAux::printError("StatsServerCache: added server for %s",
                                 record->serverName.c_str());
      }
      if (allocServer->loadAddr() < 0) {
        // Something wrong with this address, skip this entry.
        badServers.insert(badServerKey);
        delete allocServer;
        continue;
      }

      newServers->push_back(allocServer);
      server = newServers->end() - 1;
    }

    // The contents of qualTblName is one of:
    //   * db when numDots = 0
    //   * db.table when numDots = 1
    //   * db.table.index when numDots = 2
    std::string qualTblName;
    std::string tableName;
    std::string indexName;

    GsName nameType = classifyName(record->dbName, record->tblName,
                                   &qualTblName, &tableName, &indexName);

    // Save the components (db, table, index) from db.table.index so
    // that the string db.table.index doesn't have to be split again.
    if (nameType != GSN_ERR &&
        qualNameToParts->find(qualTblName) == qualNameToParts->end()) {
      std::vector<std::string> parts;
      parts.push_back(record->dbName);
      parts.push_back(tableName);
      parts.push_back(indexName);
      (*qualNameToParts)[qualTblName] = parts;
      if (googlestats_log_level >= GsLogLevelMed) {
        StatsServerAux::printError("processServerRecords %s is %s,%s,%s",
                                   qualTblName.c_str(), record->dbName.c_str(),
                                   tableName.c_str(), indexName.c_str());
      }
    }

    switch (nameType) {
      case GSN_DB:
        // The name is 'db'. A wildcard server.
        addToTblMap(qualTblName, record->tier, false, *server, wildcardServers);
        break;
      case GSN_DB_TABLE:
        // The name is 'db.table'. Sharded by table.
        addToTblMap(qualTblName, record->tier, false, *server, byTableServers);
        break;
      case GSN_DB_TABLE_INDEX:
        // The name is 'db.table.index'. Sharded by index.
        addToTblMap(qualTblName, record->tier, false, *server, byIndexServers);
        break;
      default:
        continue;
    }
  }
  if (googlestats_log_level >= GsLogLevelMed) {
    StatsServerAux::printError("StatsServerCache: found %d servers",
                               (int)newServers->size());
  }
  return true;
}

int
StatsServerCache::getFakeEntries(
    const TblMap& fromServers,
    const TblMap& toServers,
    const int prefixLength,
    const std::map<std::string, std::vector<std::string> >& qualNameToParts,
    std::vector<std::pair<TblMapKey, TableAtServer> >* insertItems) {
  const char* kWho = "StatsServerCache::getFakeEntries";

  // For each entry in fromServers, find matching servers in toServers and
  // create fake entries. See the class comment in gss_cache.h for details.
  TblMap::const_iterator toEntry;
  for (toEntry = toServers.begin(); toEntry != toServers.end(); ++toEntry) {

    // Get the components of the name db, table, index from the key. The
    // key is one of 'db', 'db.table', or 'db.table.index'.
    std::map<std::string, std::vector<std::string> >::const_iterator
      partIter = qualNameToParts.find(toEntry->first.first);

    if (partIter == qualNameToParts.end()) {
      StatsServerAux::printError("%s: cannot find parts for %s",
                                 kWho, toEntry->first.first.c_str());
      return -1;
    }
    const std::vector<std::string>& parts = partIter->second;
    std::string matchName;

    if (parts.size() != 3) {
      StatsServerAux::printError("%s: bad name components for %s",
                                 kWho, toEntry->first.first.c_str());
      return -1;
    }

    switch (prefixLength) {
      case 0:
        matchName = parts[0];
        break;
      case 1:
        matchName = parts[0] + "." + parts[1];
        break;
      default:
        StatsServerAux::printError("%s: prefixLength must != %d",
                                   kWho, prefixLength);
        return -1;
    }
    if (googlestats_log_level >= GsLogLevelMed) {
      StatsServerAux::printError("%s: prefixLength=%d, key=%s from %s.%s.%s",
                                 kWho, prefixLength, matchName.c_str(),
                                 parts[0].c_str(), parts[1].c_str(),
                                 parts[2].c_str());
    }

    // Determine if there is an entry in fromServers for the same database
    // and tier.
    TblMapKey fromKey(matchName, toEntry->first.second);
    TblMap::const_iterator fromIter = fromServers.find(fromKey);
    if (fromIter == fromServers.end()) {
      if (googlestats_log_level >= GsLogLevelMed)
        StatsServerAux::printError("%s: has no matches", kWho);
      continue;
    }

    // For all tables from non-wildcard entries that have a matching database
    // name and tier, prepare entries to be inserted for the wildcard entry.
    // These entries use the tablenames from the non-wildcard entries.
    std::vector<TableAtServer>::const_iterator tableIter;
    tableIter = fromIter->second.begin();
    for (; tableIter != fromIter->second.end(); ++tableIter) {
      // Save info about the matching wildcard server.
      TableAtServer tableAtServer(tableIter->server, 0);
      insertItems->push_back(std::make_pair(toEntry->first, tableAtServer));
      if (googlestats_log_level >= GsLogLevelMed) {
        StatsServerAux::printError("%s: matches %s:%d", kWho,
                                   tableIter->server->name.c_str(),
                                   tableIter->server->port);
      }
    }
  }
  return 0;
}

void StatsServerCache::addAllEntries(const TblMap& myMap) {
  TblMap::const_iterator mapIter = myMap.begin();
  for (; mapIter != myMap.end(); ++mapIter) {
    std::vector<TableAtServer>::const_iterator tableIter =
      mapIter->second.begin();

    for (; tableIter != mapIter->second.end(); ++tableIter) {
      // Save info about the matching wildcard server.
      addToTblMap(mapIter->first.first, mapIter->first.second, true,
                  tableIter->server, &tblMap);
    }
  }
}

bool
StatsServerCache::initCache(const ServerRecordVector& serverRecords) {
  // See gss_cache.h for a description of the structure of the cache.
  const char* kWho = "StatsServerCache::initCache";

  (void) pthread_mutex_lock(&mutex);

  TblMap wildcardServers;  // Wildcard entries from LocalStatsServers.
  TblMap byTableServers;   // LocalStatsServers entries sharded by table.
  TblMap byIndexServers;   // LocalStatsServers entries sharded by index.

  // Maps the qualified table name to its components (db, table, index)
  std::map<std::string, std::vector<std::string> > qualNameToParts;

  // Scan serverRecords and classify entries as:
  //   * wildcardServers: TableName == ''
  //   * byTableServers: TableName == 'table'
  //   * byIndexServers: TableName == 'table.index'
  ServerInfoPtrVector statsServers;
  if (!processServerRecords(serverRecords, &statsServers,
                            &qualNameToParts, &wildcardServers,
                            &byTableServers, &byIndexServers)) {
    clearServerRecords(&statsServers);
    return false;
  }

  // Entries for wildcard servers to be added to tblMap.
  std::vector<std::pair<TblMapKey, TableAtServer> > insertItems;

  // Create fake table@db@server entries from db@server entries.
  if (getFakeEntries(wildcardServers, byTableServers, 0, qualNameToParts,
                     &insertItems)) {
    StatsServerAux::printError("%s: failed for db to table mapping", kWho);
    clearServerRecords(&statsServers);
    return false;
  }

  // Create fake index@table@db@server entries from db@server entries.
  if (getFakeEntries(wildcardServers, byIndexServers, 0, qualNameToParts,
                     &insertItems)) {
    StatsServerAux::printError("%s: failed for db to index mapping", kWho);
    clearServerRecords(&statsServers);
    return false;
  }

  // Create fake index@table@db@server entries from table@db@server entries.
  if (getFakeEntries(byTableServers, byIndexServers, 1, qualNameToParts,
                     &insertItems)) {
    StatsServerAux::printError("%s: failed for table to index mapping", kWho);
    clearServerRecords(&statsServers);
    return false;
  }

  // Delay clearing current cache state until this is guaranteed to succeed.
  clear();
  servers = statsServers;
  if (googlestats_log_level >= GsLogLevelMed) {
    StatsServerAux::printError("StatsServerCache: has %d servers",
                               (int)servers.size());
  }

  // Now insert the wildcard entries for each db
  addAllEntries(byIndexServers);
  addAllEntries(byTableServers);
  addAllEntries(wildcardServers);

  // Insert the wildcard entries into the tblMap for each db, table pair.
  std::vector<std::pair<TblMapKey, TableAtServer> >::iterator insertIter;
  insertIter = insertItems.begin();
  for (; insertIter != insertItems.end(); ++insertIter) {
    // The non-wildcard entries must already be in the map.
    DBUG_ASSERT(tblMap.find(insertIter->first) != tblMap.end());

    // Add the wildcard entry.
    tblMap[insertIter->first].push_back(insertIter->second);
    if (googlestats_log_level >= GsLogLevelLow) {
      StatsServerAux::printError("StatsServerCache: add wildcard server "
                                 "for %s at %s:%d to tier %d",
                                 insertIter->first.first.c_str(),
                                 insertIter->second.server->name.c_str(),
                                 insertIter->second.server->port,
                                 insertIter->first.second);
    }
  }

  pthread_mutex_unlock(&mutex);
  return true;
}
