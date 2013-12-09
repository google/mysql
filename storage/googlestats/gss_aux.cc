// Copyright 2013 Google Inc. All Rights Reserved.

#include <string>
#include <vector>

#include "my_global.h"
#include "mysqld.h"
#include "sql_base.h"
#include "lock.h"
#include <m_ctype.h>
#include "gss_aux.h"
#include "gss_errors.h"
#include "ha_googlestats.h"
#include "status_vars.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/utsname.h>
#include <sys/poll.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

ulong googlestats_write_tries = 1;

int get_versions_for_googlestats_tables(THD* thd, TABLE_LIST* tables) {
  return StatsServerAux::getVersionNumber(thd, tables);
}

int
StatsServerAux::readBytes(
  int sock_fd,
  const char* server_name,
  void* buf,
  size_t length,
  int timeout)
{
  server_name = server_name ? server_name : "unknown";
  size_t total = 0;
  while (total < length) {
    bool timedOut;
    if (waitForSocket(sock_fd, server_name, timeout, false, &timedOut) < 0 ||
        timedOut) {
      statistic_increment(google_network_io_failures, &LOCK_stats);
      printError("readBytes: time-out or error on socket from %s (errno=%d, "
                 "total=%d, requested=%d)", server_name, errno, (int)total,
                 (int)length);
      return timedOut ? GSS_ERR_TIMEOUT : GSS_ERR_SOCKET_READ;
    }

    // read rows
    int len = read(sock_fd, buf, length - total);
    if (len < 0) {
      statistic_increment(google_network_io_failures, &LOCK_stats);
      printError("readBytes: read error on socket from %s (errno=%d, len=%d, "
                 "total=%d, requested=%d)",
                 server_name, errno, len, (int)total, (int)length);
      return(GSS_ERR_SOCKET_READ);
    }
    if (length == 0 && total == 0) {
      // huh?
      printError("length == 0 && total == 0");
    }
    if (len == 0) {
      statistic_increment(google_network_io_failures, &LOCK_stats);
      printError("readBytes: socket unexpectedly closed from %s "
                 "(total=%d, requested=%d)", server_name, (int)total,
                 (int)length);
      return(GSS_ERR_SOCKET_READ);
    }
    total += len;
    buf = static_cast<void*>(static_cast<char*>(buf) + len);
    if (googlestats_log_level >= GsLogLevelHigh) {
      printError("readBytes: read %d bytes", len);
    }
  }
  return(0);
}

// return -1 if any error or timeout; return 0 otherwise
int
StatsServerAux::waitForSocket(
  int sock_fd,
  const char* server_name,
  int timeout_sec,
  bool waitForWrite,
  bool* timedOut)
{
  *timedOut = false;
  server_name = server_name ? server_name : "unknown";

  struct pollfd poll_fd;
  poll_fd.fd = sock_fd;
  poll_fd.events = POLLIN | POLLPRI;
  if (waitForWrite) {
    poll_fd.events |= POLLOUT;
  }

  struct timeval stime, etime;
  if (gettimeofday(&stime, 0) < 0) {
    printError("waitForSocket: error in gettimeofday() (errno = %d)", errno);
  }

  int n = poll(&poll_fd, 1, timeout_sec * 1000);
  if (gettimeofday(&etime, 0) < 0) {
    printError("waitForSocket: error in gettimeofday() (errno = %d)", errno);
  }

  // figure out how many milliseconds this took
  int time_in_ms = (etime.tv_sec - stime.tv_sec) * 1000
    + (etime.tv_usec - stime.tv_usec) / 1000;

  if (googlestats_log_level >= GsLogLevelMed && time_in_ms > 1000) {
    printError("waitForSocket: long poll from %s (%d ms), events = %d",
               server_name, time_in_ms, poll_fd.events);
  }

  if (n == 0) {
    // time-out
    printError("waitForSocket: timing out from %s after %dms",
               server_name, time_in_ms);
    *timedOut = true;
    return(-1);
  }
  if (n == -1) {
    // some error
    printError("waitForSocket: error in poll from %s (errno = %d)",
               server_name, errno);
    return(-1);
  }

  if (n != 1) {
    printError("waitForSocket: poll from %s returns wrong number of fds "
               "with events (%d)", server_name, n);
  }

  if (!(poll_fd.revents & (POLLIN | POLLPRI))
    && !(waitForWrite && (poll_fd.revents & POLLOUT)))
  {
    printError("waitForSocket: poll from %s returns for wrong event "
               "(event = %d)", server_name, poll_fd.revents);
    return(-1);
  }
  if (poll_fd.revents & POLLPRI) {
    printError("waitForSocket: poll() from %s returned with POLLPRI",
               server_name);
  }

  // check for pending socket errors, which we also treat as time-outs
  int error;
  socklen_t len;
  len = sizeof(error);
  if (getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 ||
      error != 0) {
    printError("waitForSocket: error from %s on socket (%d)",
               server_name, error);
    *timedOut = true;
    return(-1);
  } else {
    return(0);
  }
}


// print value stored in field;
// this assumes that the value got copied to field::ptr prior to calling
// this function, for instance by doing a scan of a particular index
void
StatsServerAux::printColVal(
  const uchar* row,
  struct st_key_part_info* part_info,
  std::string* out)
{
  const uchar* val_ptr = row + part_info->offset;
  Field* field = part_info->field;
  memcpy(field->ptr, val_ptr, part_info->length);
  printColVal(field, out);
}


// print value stored in field;
// this assumes that the value got copied to field::ptr prior to calling
// this function, for instance by doing a scan of a particular index
void
StatsServerAux::printColVal(
  Field* field,
  std::string* out)
{
  // field->unpack(field->ptr, val_ptr);
  if (field->is_null()) {
    out->append("NULL");
    return;
  }
  String val;
  field->val_str(&val, &val);
  out->append(val.c_ptr_safe());
}

// Open and lock the version table (CommittedStatsVersions). Set *tbl and
// *lock to 0 on failure. Return 0 on success.
int StatsServerAux::lockVersionTable(const char* dbName,
                                     const char* tblName,
                                     TABLE** tbl,
                                     MYSQL_LOCK** lock)
{
  DBUG_ENTER("StatsServerAux::lockVersionTable");
  *tbl = 0;
  *lock = 0;

  // Save proc_info, as open_tables() sets it to 0.
  const char* proc_info = current_thd->proc_info;
  const int kMaxName = NAME_LEN;
  char dbStr[kMaxName+1], tblStr[kMaxName+1];
  strncpy(dbStr, dbName, kMaxName);
  strncpy(tblStr, tblName, kMaxName);

  TABLE_LIST table_list;
  table_list.init_one_table(dbStr, strlen(dbStr),
                            tblStr, strlen(tblStr),
                            tblStr, TL_READ);

  TABLE_LIST* table_list_ptr = &table_list;
  uint ctr;
  if (open_tables(current_thd, &table_list_ptr, &ctr, 0)) {
    printError("lockVersionTable: Can't open table %s.%s", dbName, tblName);
    DBUG_RETURN(-1);
  }
  *tbl = table_list.table;

  *lock = mysql_lock_tables(current_thd, tbl, 1, 0);
  if (*lock == 0) {
    printError("lockVersionTable: Can't lock table %s.%s", dbName, tblName);
    DBUG_RETURN(-1);
  }

  (*tbl)->file->init_table_handle_for_HANDLER();

  // TODO(mcallaghan): Should this use the macro?
  // Restore proc_info to previous value.
  current_thd->proc_info = proc_info;

  // Mark all columns to be fetched by setting bitmap of all of the
  // fields in this table.
  for (uint i = 0; i < (*tbl)->s->fields; ++i)
    bitmap_set_bit((*tbl)->read_set, (*tbl)->field[i]->field_index);

  DBUG_RETURN(0);
}

// Read the version number and hash for the table named by search_tbl from
// the stats version table represented by version_tbl. Return 0 on success.
// On success, googlestats_version_num, googlestats_version_hash and
// googlestats_query_id are set.
int StatsServerAux::queryVersionNumber(THD* thd,
                                       TABLE* version_tbl,
                                       TABLE_LIST* search_tbl)
{
  DBUG_ENTER("StatsServerAux::queryVersionNumber");
  const char* kWho = "gss_aux::queryVersionNumber";
  const char* version_tbl_name = version_tbl->s->table_name.str;
  const char* search_db_name = search_tbl->table->s->db.str;
  const char* search_tbl_name = search_tbl->table->s->table_name.str;
  DBUG_PRINT("enter", ("using %s for %s", version_tbl_name, search_tbl_name));
  std::string versionNum, versionHash; // declare before goto's
  std::string test;

  // Check whether we have primary key.
  if (version_tbl->s->keys == 0) {
    printError("%s: version table does not have primary key", kWho);
    DBUG_RETURN(-1);
  }

  // This is the size of CommittedStatsVersions.TableName + 2.
  // gss_cache.cc confirms that CommittedStatsVersions looks OK.
  // TableName must be varchar(128).
  const int tblNameSize =  130;
  if (version_tbl->field[0]->pack_length() != 129) {
    DBUG_ASSERT(0); // Guaranteed by gss_cache.cc
    printError("%s: pack length of table name is %d and must be 129",
               kWho, tblNameSize);
    DBUG_RETURN(-1);
  }

  // Read index record.
  uchar* record = version_tbl->record[0];
  if (version_tbl->file->ha_index_init(0, 1)) {
    printError("%s: unable to initialize index for the table %s.%s", kWho,
               version_tbl->s->db.str, version_tbl_name);
    DBUG_RETURN(-1);
  }

  int res = -1;
  // Prefer stack frame allocation here because it's faster than malloc.
  // Allocate 2 extra byte to guarantee that the string can be null terminated
  // and there's room for the length (see below).
  char key[tblNameSize + 2];
  int len;   // The length of the string in key, excluding trailing blanks.
  len = RemoveTrailing2sAndDate(search_tbl_name);
  if (len <= 0) {
    printError("%s: table name %s.%s is empty after replacing the schema "
               "version and trailing 2s", kWho, search_db_name,
               search_tbl_name);
    goto cleanup_scan;
  }
  if (len >= tblNameSize) {
    printError("%s: table name %s.%s is too long", kWho, search_db_name,
               search_tbl_name);
    goto cleanup_scan;
  }

  // The new (5.x) format for varchar is (len)(data) with sizeof(len) == 2
  key[0] = (char) len;
  key[1] = 0;
  memset(key + 2, 0, tblNameSize);
  strncpy(key + 2, search_tbl_name, len);
  key[tblNameSize + 1] = '\0';

  int status;
  status = version_tbl->file->ha_index_read_map(record,
                                                (uchar*)key,
                                                make_prev_keypart_map(1),
                                                HA_READ_KEY_EXACT);
  if (status < 0) {
    printError("%s: couldn't read version number for %s.%s using the edited "
               "name %s", kWho, search_db_name, search_tbl_name, key + 2);
    goto cleanup_scan;
  }

  if (status == HA_ERR_KEY_NOT_FOUND) {
    // Couldn't find anything for this table, look for a wildcard entry ('').
    memset(key, 0, tblNameSize + 2);
    status = version_tbl->file->ha_index_read_map(record,
                                                  (uchar*)key,
                                                  make_prev_keypart_map(1),
                                                  HA_READ_KEY_EXACT);
    if (status < 0) {
      printError("%s: couldn't read version number for %s.%s using the edited "
                 "name %s", kWho, search_db_name, search_tbl_name, key + 2);
      goto cleanup_scan;
    }

    if (status == HA_ERR_KEY_NOT_FOUND) {
      printError("%s: couldn't find version number for %s.%s using the edited "
                 "name %s", kWho, search_db_name, search_tbl_name, key + 2);
      goto cleanup_scan;
    }
  }
  res = 0;
  printColVal(version_tbl->field[1], &versionNum);
  printColVal(version_tbl->field[2], &versionHash);
  search_tbl->table->googlestats_version_num = atoi(versionNum.c_str());
  search_tbl->table->googlestats_version_hash = atoll(versionHash.c_str());
  search_tbl->table->googlestats_version_query_id = thd->query_id;

cleanup_scan:
  if (version_tbl->file->ha_index_end()) {
    printError("%s: error closing %s.%s", kWho, version_tbl->s->db.str,
               version_tbl->s->table_name.str);
    DBUG_RETURN(-1);
  }

  DBUG_RETURN(res);
}

// End a scan of the version table by unlocking it.
void StatsServerAux::endVersionScan(TABLE* tbl, MYSQL_LOCK* lock)
{
  DBUG_ASSERT(tbl && lock);
  if (tbl == 0 && lock != 0) {
    printError("endVersionScan: tbl == 0 && lock != 0");
  }
  if (tbl != 0 && lock != 0) {
    tbl->file->init_table_handle_for_HANDLER();
    mysql_unlock_tables(current_thd, lock);
  }
}

/*
   Set the version numbers for all googlestats tables.

   This sets the following in TABLE_LIST::table for each googlestats table.
     googlestats_query_id
     googlestats_version_num
     googlestats_version_hash

   This is done here because CommittedStatsVersions must be opened, locked and
   queried. The table locking protocol does not allow open_and_lock_tables()
   calls to be done as, because it can lead to crashes and/or deadlock.
     open_and_lock_tables() // for all tables in the query
     open_and_lock_tables() // for CommittedStatsVersions
     unlock_tables()        // for CommittedStatsVersions
     unlock_tables()        // for all tables in the query

   Therefore, the following is done:
     open_and_lock_tables() // for all tables in the query
       open_tables()        // for all tables in the query
         open_tables()      // for CommittedStatsVersions
         lock_tables()      // for CommittedStatsVersions
         query              // for CommittedStatsVersions
         unlock_tables()    // for CommittedStatsVersions
       lock_tables()        // for all tables in the query
     unlock_tables()        // for all tables in the query

   The alternative is to add CommittedStatsVersions to the end of the TABLE_LIST
   for all statements that use a googlestats tables. Unfortunately, the
   data structures are too complex and the effects of such a change are not
   understood.
*/
int StatsServerAux::getVersionNumber(THD* thd, TABLE_LIST* tables)
{
  DBUG_ENTER("StatsServerAux::getVersionNumber");
  TABLE* tbl = 0;
  MYSQL_LOCK* lock = 0;
  const char* dbName = 0;
  const char* kWho = "gss_aux::getVersionNumber";

  TABLE_LIST* iter;
  for (iter = tables; iter; iter = iter->next_global) {
    if (iter->table) {
      iter->table->googlestats_version_query_id = 0;
      iter->table->googlestats_version_num = -1;
      iter->table->googlestats_version_hash = -1;
    }
  }

  bool has_googlestats = false;
  for (iter = tables; iter; iter = iter->next_global) {
    if (iter->table &&
        ha_legacy_type(iter->table->s->db_type()) == DB_TYPE_GOOGLESTATS) {
      has_googlestats = true;
      if (!dbName) {
        dbName = iter->table->s->db.str;
      } else if (strcmp(dbName, iter->table->s->db.str)) {
        printError("getVersionNumber: multiple database names: %s, %s",
                   dbName, iter->table->s->db.str);
        DBUG_RETURN(-1);
      }
    }
  }

  if (!has_googlestats) {
    // Nothing to do when there are no googlestats tables.
    DBUG_RETURN(0);
  }

  if (googlestats_log_level >= GsLogLevelHigh) {
    StatsServerAux::printError("%s: for command (%d) query (%s)", kWho,
                               thd->lex ? thd->lex->sql_command : 0,
                               thd->query() ? thd->query() : "<no query>");
  }

  if (!googlestats_version_table) {
    printError("getVersionNumber: googlestats_version_table is not set");
    DBUG_RETURN(-1);
  }

  // Open and lock the table that contains version numbers and hashes.
  if (lockVersionTable(dbName, googlestats_version_table, &tbl, &lock)) {
    DBUG_RETURN(-1);
  }

  DBUG_ASSERT(tbl && lock);
  if (!tbl || !lock) {
    printError("lockVersionTable return OK, but tbl or lock are not set");
    DBUG_RETURN(-1);
  }

  // Get a version number and hash for each googlestats table.
  int result = -1;
  for (iter = tables; iter; iter = iter->next_global) {
    if (iter->table &&
        ha_legacy_type(iter->table->s->db_type()) == DB_TYPE_GOOGLESTATS) {
      if (queryVersionNumber(thd, tbl, iter)) {
        goto done;
      }
    }
  }
  result = 0;

done:
  // Unlock the version table.
  endVersionScan(tbl, lock);
  DBUG_RETURN(result);
}

// Print to mysql error log, just like printError; prefix with query_id.
// This code adapted from sql/log.cc.
void
StatsServerAux::printError(const char* format, ...)
{
  va_list args;
  time_t skr;
  struct tm tm_tmp;
  struct tm *start;
  va_start(args, format);
  DBUG_ENTER("StatsServerAux::printError");

#ifndef EMBEDDED_LIBRARY
  mysql_mutex_lock(&LOCK_error_log);
#endif

  skr = time(NULL);
  localtime_r(&skr, &tm_tmp);
  start = &tm_tmp;
  // TODO(jeremycole): Use 4-digit year here.
  fprintf(stderr, "%02d%02d%02d %2d:%02d:%02d %llu ",
          start->tm_year % 100,
          start->tm_mon + 1,
          start->tm_mday,
          start->tm_hour,
          start->tm_min,
          start->tm_sec,
          (current_thd ? current_thd->query_id : 0ULL));
  (void) vfprintf(stderr, format, args);
  (void) fputc('\n', stderr);
  fflush(stderr);
  va_end(args);

#ifndef EMBEDDED_LIBRARY
  mysql_mutex_unlock(&LOCK_error_log);
#endif

  DBUG_VOID_RETURN;
}

struct hostent*
StatsServerAux::getHostByName(const char* name, struct hostent* host,
                              char* buf, int buf_len)
{
  struct hostent* result = NULL;
  int error = 0;
  // Return value ignored because error return checked via 'result'
  gethostbyname_r(name, host, buf, buf_len, &result, &error);
  if (result) {
    return result;
  } else {
    const char* msg;
    switch (error) {
      case HOST_NOT_FOUND:
        msg = "HOST_NOT_FOUND";
        break;
      case TRY_AGAIN:
        msg = "TRY_AGAIN";
        break;
      case NO_RECOVERY:
        msg = "NO_RECOVERY";
        break;
      case NO_ADDRESS:
        msg = "NO_ADDRESS";
        break;
      default:
        msg = "UNKNOWN RESULT";
        break;
    }
    StatsServerAux::printError("StatsServerCache: couldn't find host '%s'"\
                               " with error %s (%d)", name, msg, error);
    return NULL;
  }
}

int StatsServerAux::RemoveTrailing2sAndDate(const char* table_name) {
  int len;
  const char* last_dollar = rindex(table_name, '$');
  if (last_dollar) {
    len = last_dollar - table_name;
  } else {
    len = strlen(table_name);
  }
  // Skip the trailing '2's.
  while (len > 0 && table_name[len - 1] == '2') {
    len--;
  }
  return len;
}

int StatsServerAux::Write(int fd, const char* data, int len) {
  statistic_increment(google_requests, &LOCK_stats);
  int total_bytes_written = 0;
  int bytes_left = len;
  for (unsigned int iterations = 0; iterations < googlestats_write_tries;
       ++iterations) {
    int wrote = write(fd, data + total_bytes_written, bytes_left);
    if (wrote < 0) {
      StatsServerAux::printError("Tried to write %d bytes to fd %d, "
                                 "actually wrote %d with errno %d",
                                 len, total_bytes_written, fd, errno);
      break;
    }
    total_bytes_written += wrote;
    bytes_left -= wrote;
    // write() could have failed, but EINPROGRESS is not fatal.
    // Only allow a limited number of retries.
    if (bytes_left && (errno != EINPROGRESS ||
                       (iterations + 1) >= googlestats_write_tries)) {
      StatsServerAux::printError("Tried to write %d bytes to fd %d, "
                                 "actually wrote %d with errno %d",
                                 len, total_bytes_written, fd, errno);
      break;
    }

    if (total_bytes_written == len) {
      // All the requested bytes have been written to the socket.
      break;
    }

    // Wait for the socket to be ready for more data.
    bool timedOut;
    if (waitForSocket(fd, NULL, 1, true, &timedOut) < 0 || timedOut) {
      StatsServerAux::printError("Tried to write %d bytes to fd %d, "
                                 "actually wrote %d with errno %d",
                                 len, total_bytes_written, fd, errno);
      break;
    }
  }

  return total_bytes_written;
}

void StatsServerAux::EscapeString(const char* key_buf, int key_length,
                                  std::string *out) {
  out->reserve(out->size() + key_length * 2 + 1);

  for (int idx = 0; idx < key_length; ++idx) {
    // Escape: '\n' is escaped to '\\', 'n' (this is request by stats server)
    if (key_buf[idx] == '\n') {
      out->append(1, '\\');
      out->append(1, 'n');
    // Escape: '\r' is escaped to '\\', 'r'
    } else if (key_buf[idx] == '\r') {
      out->append(1, '\\');
      out->append(1, 'r');
    // Escape: '\0' is escaped to '\\', '0'
    } else if (key_buf[idx] == '\0') {
      out->append(1, '\\');
      out->append(1, '0');
    } else {
      // Escape: space, ',' , '\\'
      if (key_buf[idx] == ',' || key_buf[idx] == '\\' || key_buf[idx] == ' ') {
        out->append(1, '\\');
      }
      out->append(1, key_buf[idx]);
    }
  }
}

void StatsServerAux::EscapeInCondString(const char* key_buf, int key_length,
                                        std::string *out) {
  out->reserve(out->size() + key_length * 2 + 1);

  for (int idx = 0; idx < key_length; ++idx) {
    // Escape: '\\', '|'
    if (key_buf[idx] == '\\' || key_buf[idx] == '|') {
      out->append(1, '\\');
    }
    out->append(1, key_buf[idx]);
  }
}
