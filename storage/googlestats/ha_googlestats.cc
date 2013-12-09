// Copyright 2013 Google Inc. All Rights Reserved.

#include <algorithm>
#include <vector>
#include <string>
#include <cctype>
#include <map>

#include <my_global.h>
#include "mysqld.h"
#include <assert.h>
#include <m_ctype.h>

#include "gss_cache.h"
#include "gss_aux.h"
#include "gss_errors.h"
#include "ha_googlestats.h"
#include "lzo/lzo1x.h"
#include "status_vars.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <sys/times.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>

// The stats server protocol is defined by its implementation in the stats server and
// this table handler. In some cases, native integers (4 and 8 byte) are returned
// by the stats server. When this is done, the value is returned in the native
// little endian (x86) format and uses 4 or 8 bytes. Constants are used to make it
// easier to find when native ints are returned.
#define INT_4_BYTES 4
#define INT_8_BYTES 8

#define TBLNAME_SIZE NAME_LEN         // The size of GoogleStatsVersion.TableName.

#define MIN_NUM_ROWS_TO_FETCH 64
#define MAX_NUM_ROWS_TO_FETCH 16 * 1024

ulonglong google_fetch = 0;
ulonglong google_fetch_mics = 0;
ulonglong google_fetch_mics_per = 0;

ulonglong google_nonfetch = 0;
ulonglong google_nonfetch_mics = 0;
ulonglong google_nonfetch_mics_per = 0;

char *googlestats_servers_table = NULL; // Usually "LocalStatsServers".
char *googlestats_version_table = NULL; // Usually "CommittedStatsVersions".

int googlestats_timeout = 0;
int googlestats_retry_interval = 0;
int googlestats_log_level = 0;

// Count failed connection attempts. A request for a connection
// for a user session may increment this 1 or more times.
ulong google_connect_failures = 0;

// Count failures to get a connection for a session. A request
// for a connection for a user session increments this at most once.
ulong google_connect_not_possible = 0;

// Count read and write failures from stats server RPCs.
ulong google_network_io_failures = 0;

// Count number of rows and bytes fetched from a stats server.
ulonglong google_fetch_rows = 0;
ulonglong google_fetch_bytes = 0;

// Count the number of connections to a tierN stats server.
ulong google_connect_tier0 = 0;
ulong google_connect_tier1 = 0;
ulong google_connect_tier2 = 0;
ulong google_connect_tier3 = 0;
ulong google_connect_tier4 = 0;
ulong google_connect_tier5 = 0;
ulong google_connect_tier6 = 0;
ulong google_connect_tier7 = 0;
ulong google_connect_tier8 = 0;
ulong google_connect_tier9 = 0;

// Count the number of requests to a tierN stats server.
ulong google_fetch_tier0 = 0;
ulong google_fetch_tier1 = 0;
ulong google_fetch_tier2 = 0;
ulong google_fetch_tier3 = 0;
ulong google_fetch_tier4 = 0;
ulong google_fetch_tier5 = 0;
ulong google_fetch_tier6 = 0;
ulong google_fetch_tier7 = 0;
ulong google_fetch_tier8 = 0;
ulong google_fetch_tier9 = 0;

static ulong* fetch_by_tier[] = {
  &google_fetch_tier0,
  &google_fetch_tier1,
  &google_fetch_tier2,
  &google_fetch_tier3,
  &google_fetch_tier4,
  &google_fetch_tier5,
  &google_fetch_tier6,
  &google_fetch_tier7,
  &google_fetch_tier8,
  &google_fetch_tier9
};

// Count the number of bytes fetched from a tierN stats server.
ulonglong google_fetch_bytes_tier0 = 0;
ulonglong google_fetch_bytes_tier1 = 0;
ulonglong google_fetch_bytes_tier2 = 0;
ulonglong google_fetch_bytes_tier3 = 0;
ulonglong google_fetch_bytes_tier4 = 0;
ulonglong google_fetch_bytes_tier5 = 0;
ulonglong google_fetch_bytes_tier6 = 0;
ulonglong google_fetch_bytes_tier7 = 0;
ulonglong google_fetch_bytes_tier8 = 0;
ulonglong google_fetch_bytes_tier9 = 0;

static ulonglong* fetch_bytes_by_tier[] = {
  &google_fetch_bytes_tier0,
  &google_fetch_bytes_tier1,
  &google_fetch_bytes_tier2,
  &google_fetch_bytes_tier3,
  &google_fetch_bytes_tier4,
  &google_fetch_bytes_tier5,
  &google_fetch_bytes_tier6,
  &google_fetch_bytes_tier7,
  &google_fetch_bytes_tier8,
  &google_fetch_bytes_tier9
};

// Count the number of stats servers at tierN.
ulong google_statsservers_tier0 = 0;
ulong google_statsservers_tier1 = 0;
ulong google_statsservers_tier2 = 0;
ulong google_statsservers_tier3 = 0;
ulong google_statsservers_tier4 = 0;
ulong google_statsservers_tier5 = 0;
ulong google_statsservers_tier6 = 0;
ulong google_statsservers_tier7 = 0;
ulong google_statsservers_tier8 = 0;
ulong google_statsservers_tier9 = 0;

// Count number of GoogleStats requests.
ulong google_requests = 0;

// The maximum size of a packet that the table handler will try to read
// from a stats server. Packets larger than this will be rejected and
// cause the query to fail. Set in mysqld.cc.
int googlestats_max_packet_size = 0;

// When TRUE, use a temp table to sort rows from a stats table. The alternative
// is to sort the pair (rowid, sort_key) and then fetch rows from the stats
// table by rowid. Unfortunately, that is not supported by stats tables, and an
// error will be raised. This is only needed for 4.0. The sort in 4.1+ does not
// have this problem. It sorts (row columns, sort_key) pairs.
my_bool buffer_table_sort = TRUE;

// The time interval between selectivity estimates, in seconds.
ulong googlestats_estimate_interval = 0;

// For condition pushdown, the maximum number of bytes to send to the
// GoogleStats server (0 means unlimited).
ulong googlestats_pushdown_max_bytes = 0;

// For condition pushdown, the maximum number of elements in an IN list
// (0 means unlimited).
ulong googlestats_pushdown_max_in_size = 0;

// The first tier considered "remote".  Used for logging remote connections.
ulong googlestats_remote_tier = 3;

extern ulong googlestats_write_tries;

//
// Thread local variables must be defined up here.
//
static MYSQL_THDVAR_ULONG(initial_fetch_rows,
  PLUGIN_VAR_OPCMDARG,
  "Number of rows for the initial stats FETCH",
  /* check_func */ NULL,
  /* update_func */ NULL,
  /* default */ MIN_NUM_ROWS_TO_FETCH,
  /* minimum */ MIN_NUM_ROWS_TO_FETCH,
  /* maximum */ MAX_NUM_ROWS_TO_FETCH,
  /* by      */ 0);

static MYSQL_THDVAR_ULONG(multifetch_max_keys,
  PLUGIN_VAR_OPCMDARG,
  "Maximum number of keys to send for a multifetch request "
  "(0 means unlimited)",
  /* check_func */ NULL,
  /* update_func */ NULL,
  /* default */ 100,
  /* minimum */ 0,
  /* maximum */ 2000,
  /* by      */ 0);

static MYSQL_THDVAR_BOOL(query_on_update,
  PLUGIN_VAR_OPCMDARG,
  "Allow GoogleStats tables to be used in an INSERT, UPDATE, or DELETE statement "
  "(which might be written to a binary log)",
  /* check_func */ NULL,
  /* update_func */ NULL,
  /* default */ 0);

template <typename T>
const T& gs_max(const T& a, const T& b) {
  return a > b ? a : b;
}

template <typename T>
const T& gs_min(const T& a, const T& b) {
  return a < b ? a : b;
}


// This array should be updated when the errors in gss_errors.h are updated.
static const char *gss_err_msgs[] = {
  "RPC timeout exceeded.",
  "Read error on socket.",
  "Computing schema checksum failed.",
  "Initializing index failed.  Index does not exist.",
  "Memory allocation error.",
  "Write error on socket.",
  "Received error signature from stats-server.",
  "Bad version number for GoogleStats table.",
  "No restart key, so restarts are not supported.",
  "Invalid header values from stats-server packet.",
  "Decompressing data failed.",
  "Received a bad restart key from stats-server.",
  "Failed to get buffer row.",
  "Failed to get list of request columns.",
  "Unsupported scan type.",
  "Bad table or index name.",
  "Could not connect to any stats-server for this table.",
  "Could not create socket.",
  "No stats-servers exist for this table.  Check LocalStatsServers table.",
};

inline
static bool might_write_binlog()
{
  return mysql_bin_log.is_open()
         && (current_thd->variables.option_bits & OPTION_BIN_LOG);
}

// Optimized memcpy for common field lengths.
//
// This function is implemented with a switch statement for all field lengths
// from zero through 8 (inclusive).
//
// The optimizer decides whether to use a real switch block or a series of
// iterative if/then/else blocks.  A switch block is more efficient when:
//
//    a) the number of switch values is more than 2; and
//    b) the switch values are contiguous (e.g. 4,5,6,7)
//
// The compiler generates a "jump table" based on the switch targets; e.g.
//      jmp *.table(,%eax,4)
// .table:
//      .long  case0
//      .long  case1
//      ....
//  .case0: ... // code that handles switch value 0 (relative 0)
//  .case1: ... // code that handles switch value 1 (relative 1)
//
// If the compiler chooses to generate if/then/else blocks, then it will be no
// worse than coding it yourself.  Although you have no control over which
// switch values are tested first.
//
// Adding additional contiguous switch targets has no effect on the execution
// speed (just the size of the jump table).  However if you add non-contiguous
// targets it WILL introduce additional if/then/else's for that target, but
// will not affect the execution speed of the contiguous targets.
//
// I have confirmed that this is faster for small fields (1-8 bytes) using
// gcc 3.2.2 on a P4 (mcallaghan, March 2006)
//
// TODO(jeremycole): Is this still necessary and/or wise?
inline
static void memcpy_field(uchar* to, const uchar* from, int size) {
  switch(size) {
    case 0: break;
    case sizeof(uint8):
      *to = *from;
      break;
    case sizeof(uint16):
      (* (uint16 *) to) = (* (uint16 *) from);
      break;
    case sizeof(uint16) + sizeof(char):
      (* (uint16 *) to) = (* (uint16 *) from);
      to += sizeof(uint16);
      from += sizeof(uint16);
      (* to) = (* from);
      break;
    case sizeof(uint32):
      (* (uint32 *) to) = (* (uint32 *) from);
      break;
    case sizeof(uint32) + sizeof(char):
      (* (uint32 *) to) = (* (uint32 *) from);
      to += sizeof(uint32);
      from += sizeof(uint32);
      (* to) = (* from);
      break;
    case sizeof(uint32) + sizeof(uint16):
      (* (uint32 *) to) = (* (uint32 *) from);
      to += sizeof(uint32);
      from += sizeof(uint32);
      (* (uint16 *) to) = (* (uint16 *) from);
      break;
    case sizeof(uint32) + sizeof(uint16) + sizeof(char):
      (* (uint32 *) to) = (* (uint32 *) from);
      to += sizeof(uint32);
      from += sizeof(uint32);
      (* (uint16 *) to) = (* (uint16 *) from);
      to += sizeof(uint16);
      from += sizeof(uint16);
      (* to) = (* from);
      break;
    case sizeof(ulonglong):
      (* (ulonglong *) to) = (* (ulonglong *) from);
      break;
    default:
      memcpy(to, from, size);
  }
}

// A helper class to set THD::proc_info and restore it on function exit.
//
// TODO(jeremycole): This should be updated to use THD_STAGE_INFO() and
// set up to use stages instead of strings. Manipulating proc_info like
// this is no longer safe.
class UpdateProcInfo {
 public:
  UpdateProcInfo(const char* msg) {
    // current_thd is not set when the server is shutting down
    thd_ = current_thd;
    if (thd_) {
      old_msg_ = thd_->proc_info;
      thd_proc_info(thd_, msg);
    }
  }

  ~UpdateProcInfo() {
    // current_thd is not set when the server is shutting down
    if (thd_)
      thd_proc_info(thd_, old_msg_);
  }

 private:
  THD* thd_;
  const char* old_msg_;
};

inline
static void cleanup_socket(int *fd) {
  if (*fd < 0)
    return;

  ::close(*fd);
  *fd = -1;
}

// Base timer class.
class Incrementer {
 public:
  Incrementer(ha_googlestats* handler) : handler_(handler) {
    // Set tv_sec to 0 on error.
    if (gettimeofday(&start_, 0))
      start_.tv_sec = 0;
  }

 protected:

  // Return the number of microsends. Returns 0 on backwards time travel
  // or if gettimeofday fails.
  long DiffInMics() {
    struct timeval end;

    if (!gettimeofday(&end, 0) && start_.tv_sec) {
      long diff = ((end.tv_sec - start_.tv_sec) * 1000000) +
                  (end.tv_usec - start_.tv_usec);
      return diff >= 0 ? diff : 0;
    } else {
      return 0;
    }
  }

 protected:
  struct timeval start_;
  ha_googlestats* handler_;
};

// Timer class that increments time that a handler instance spends
// on FETCH and FETCH_MORE commands.
class IncrementFetch : public Incrementer {
 public:
  IncrementFetch(ha_googlestats* handler) :
      Incrementer(handler), rows_(0), bytes_(0) { }

  ~IncrementFetch() {
    handler_->fetch_mics += DiffInMics();
    handler_->fetch_calls++;
    handler_->fetch_rows += rows_;
    handler_->fetch_bytes += bytes_;
  }

  void set_rows_and_bytes(unsigned r, unsigned b) {
    rows_ = r;
    bytes_ = b;
  }

 private:
  unsigned rows_;
  unsigned bytes_;
};

// Timer class that increments time that a handler instance spends
// on SHOW_TABLE_INFO and COUNT commands.
class IncrementNonfetch : public Incrementer {
 public:
  IncrementNonfetch(ha_googlestats* handler) : Incrementer(handler) { }
  ~IncrementNonfetch() {
    handler_->nonfetch_mics += DiffInMics();
    handler_->nonfetch_calls++;
  }
};

void ha_googlestats::cleanup() {
  if (sock_fd >= 0) {
    ::close(sock_fd);
    sock_fd = -1;
  }

  if (current_thd &&
      (fetch_calls || nonfetch_calls || fetch_mics || nonfetch_mics ||
       fetch_rows || fetch_bytes)) {
    mysql_mutex_lock(&LOCK_status);
    google_fetch          += fetch_calls;
    google_nonfetch       += nonfetch_calls;
    google_fetch_mics     += fetch_mics;
    google_nonfetch_mics  += nonfetch_mics;
    google_fetch_rows     += fetch_rows;
    google_fetch_bytes    += fetch_bytes;

    // TODO(seanrees): Can we change the type of kMaxStatsServerTier?
    if (server_tier <= (unsigned int)kMaxStatsServerTier) {
      *(fetch_bytes_by_tier[server_tier]) += fetch_bytes;
      *(fetch_by_tier[server_tier]) += fetch_calls;
    }

    fetch_calls    = 0;
    nonfetch_calls = 0;
    fetch_mics     = 0;
    nonfetch_mics  = 0;
    fetch_rows     = 0;
    fetch_bytes    = 0;
    mysql_mutex_unlock(&LOCK_status);
  }
}

// Initialize the header to 0 (by default nothing is null).
inline
static void header_init(uchar* header, int header_len) {
  if (header_len == 0) {
    // nada.
  } else if (header_len == sizeof(char)) {
    *header = 0;
  } else if (header_len == sizeof(int16)) {
    *(int16*) header = 0;
  } else {
    memset(header, 0, header_len);
  }
}

ha_googlestats::ha_googlestats(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg),
      rowCount(0),
      sock_fd(-1),
      version_num(-1),
      version_hash(0),
      schema_checksum(0),
      rows_in_tbl(0),
      sel_est_last_time(-1),
      rows_left_to_fetch(false),
      num_fetch_requests(0),
      server_name(0),
      server_port(0),
      server_tier(0),
      requested_row_buffer(0),
      requested_row_buffer_size(0),
      data_buffer(0),
      data_buffer_size(0),
      restart_row(0),
      restart_row_size(0),
      restart_bytes_used(0),
      num_buffered_rows(0),
      max_buffered_rows(0),
      num_requested_rows(0),
      num_received_rows(0),
      row_bytes(0),
      scan_received_rows(false),
      requested_row_header_length(0),
      requested_row_length(0),
      nullable_request_field_count(0),
      fetch_calls(0),
      nonfetch_calls(0),
      fetch_mics(0),
      nonfetch_mics(0),
      fetch_rows(0),
      fetch_bytes(0),
      orig_keys(0),
      orig_find_flag(HA_READ_BEFORE_KEY),
      orig_request_type(Fetch),
      pushed_conds(0),
      keys_index(0),
      keys_index_for_restart(0),
      num_requested_keys(0),
      fetch_multi_enabled(false),
      last_query_id(0),
      request_field_count(0),
      request_field_max(0),
      row_header_length(0),
      request_fields(0),
      next_requested_row(NULL),
      fixed_length_row(true),
      last_connection_error(0)
{
  if (table_arg)
    stats.mean_rec_length = table_arg->reclength;
  else
    stats.mean_rec_length = 100;

  // Assume we have 10^9 rows and no compression.
  stats.data_file_length = stats.mean_rec_length * 1000000000;

  // The optimizer uses this to estimate index access cost. An example is
  // get_index_only_read_time (which is a big hack). So we will continue
  // the hack and claim that GoogleStats blocks are 1024KB. The value is
  // low to make probing GoogleStats more expensive than InnoDB.
  stats.block_size = 1024;
}

ha_googlestats::~ha_googlestats()
{
  release_resources();
}

void ha_googlestats::release_resources() {
  delete [] server_name;
  delete orig_keys;
  delete [] requested_row_buffer;
  delete [] data_buffer;
  delete [] restart_row;
  delete [] request_fields;
  delete pushed_conds;

  server_name = 0;
  orig_keys = 0;
  requested_row_buffer = 0;
  data_buffer = 0;
  restart_row = 0;
  request_fields = 0;
  pushed_conds = 0;

  version_num = -1;
  version_hash = -1;

  cleanup();
}

const char **
ha_googlestats::bas_ext() const
{
  static const char *ext[]= { "", NullS };
  return ext;
}

int
ha_googlestats::open(
  const char *name,
  int mode,
  uint test_if_locked)
{
  DBUG_ENTER("ha_googlestats::open");
  DBUG_PRINT("enter", ("table %s", name));
  UpdateProcInfo updater("ha_googlestats::open");

  // Initialize table lock. Our table handler ignores all locks,
  // hence we do not need to share the lock object across threads.
  thr_lock_init(&thread_lock);
  thr_lock_data_init(&thread_lock, &lock, NULL);
  DBUG_RETURN(0);
}

int
ha_googlestats::close(void)
{
  UpdateProcInfo updater("ha_googlestats::close");
  thr_lock_delete(&thread_lock);
  release_resources();
  return 0;
}

int
ha_googlestats::create(
  const char *name,
  TABLE *form,
  HA_CREATE_INFO *info)
{
  // TODO: Make sure stats server already has this table, and
  // that the schema is the same.
  return 0;
}

int
ha_googlestats::info(uint flag)
{
  DBUG_ENTER("ha_googlestats::info");
  const char* kWho = "ha_googlestats::info";
  UpdateProcInfo updater(kWho);

  if (flag & HA_STATUS_VARIABLE) {
    // Release latches before network activity.
    ha_release_temporary_latches(current_thd);

    // Update the "variable" part of the info.
    // Ignore errors here, if there are any, we'll just ask again later.
    get_sel_estimates();
    stats.records = rows_in_tbl;
    stats.deleted = 0;
    stats.mean_rec_length = table->s->reclength;
    stats.data_file_length = (longlong) rows_in_tbl * stats.mean_rec_length;
    stats.index_file_length = 0;
    stats.delete_length = 0;
  }

  if (flag & HA_STATUS_CONST) {
    // Update the "constant" part of the info.
    stats.max_data_file_length = 0;
  }

  if (googlestats_log_level > GsLogLevelLow) {
    printError("%s: stats records %lld, record_length %lu ",
               kWho, (long long int)stats.records, stats.mean_rec_length);
  }
  DBUG_RETURN(0);
}

// This returns the cost in number of IOs to scan the table. Because
// IOs are not done directly by the table handler, this code converts
// the cost to fetch rows over the network into an IO cost. The result
// is: 10 + kIoNoCache * nfc,  where nfc is the number of FETCH commands.
// And the number of fetch commands is #records / fetch_size. To simplify
// the computation, this assumes all fetches are for 16384 rows. 10
// is added to this value to make sure that the result is never too small.
// The value is scaled by kIoNoCache because there is limited cache benefit
// for GoogleStats rows as all data must be fetched from a stats server as
// there is not buffer cache on the mysqld server for GoogleStats data.
// This is done to make the cost comparable to the cost from InnoDB which
// does benefit from buffer caches, but does not adjust costs for that
// benefit.
double
ha_googlestats::scan_time() {
  return ulonglong2double(10 + kIoNoCache * (stats.records / 16384));
}

// Rows are fetched in increments of 64, 128, 256, ..., 16384
// (FETCH 64, FETCH_MORE 128, ...., FETCH_MORE 16384
// The Stats Server streams responses back with up to 256 rows
// per response. Code elsewhere in the table handler reads the
// header and the following 256 or less rows from each response.
//
// This code assumes that each FETCH and FETCH_MORE command
// has the cost equivalent to 1 IO. GoogleStats is likely to get
// the latency of an inter-datacenter RPC (~1 millisecond) on the
// first packet. This is close enough to the latency of a disk read.
// Finally, the per row costs are multiplied by a fudge factor (kIoNoCache)
// to adjust the results relative to InnoDB. InnoDB benefits more
// from buffer caches but does not adjust its costs to reflect that.
// GoogleStats always fetches from a Stats Server (no caching on the
// MySQL side, some caching in the OS buffer cache on the other side).
//
// All GoogleStats indexes are clustered.
double
ha_googlestats::read_time(
  uint index,
  uint ranges,
  ha_rows rows)
{
  double row_cost_per_range;

  if (ranges < 1)
    ranges = 1;

  double rows_per_range = rows / (double) ranges;

  // Compute the cost to get rows for one range.
  if (rows_per_range <= 64) {
    row_cost_per_range = 1;
  } else if (rows_per_range <= (64 + 128)) {
    row_cost_per_range = 2;
  } else if (rows_per_range <= (64 + 128 + 256)) {
    row_cost_per_range = 3;
  } else if (rows_per_range <= (64 + 128 + 256 + 512)) {
    row_cost_per_range = 4;
  } else if (rows_per_range <= (64 + 128 + 256 + 512 + 1024)) {
    row_cost_per_range = 5;
  } else if (rows_per_range <= (64 + 128 + 256 + 512 + 1024 + 2048)) {
    row_cost_per_range = 6;
  } else if (rows_per_range <= (64 + 128 + 256 + 512 + 1024 + 2048 + 4096)) {
    row_cost_per_range = 7;
  } else if (rows_per_range <= (64 + 128 + 256 + 512 + 1024 + 2048 + 4096 +
                                8192)) {
    row_cost_per_range =  8;
  } else if (rows_per_range <= (64 + 128 + 256 + 512 + 1024 + 2048 + 4096 +
                                8192 + 16384)) {
    row_cost_per_range = 9;
  } else {
    rows_per_range -= 64 + 128 + 256 + 512 + 1024 + 2048 + 4096 + 8192 + 16384;
    row_cost_per_range = 9 + (rows_per_range / 16384) + 1;
  }

  // Adjust the cost to get rows for all ranges.
  double row_cost = row_cost_per_range * ranges;

  // The total cost is the cost to probe each range plus the cost to fetch all
  // rows from that range. Multiply this by a fudge factor. This should boost the
  // cost for queries that do many probes to a GoogleStats table and make it more
  // likely that the table is used earlier in the join order. It also compensates
  // for the lack of a buffer cache for GoogleStats tables. InnoDB has one but does
  // not attempt to reduce its costs because of the cache benefit.
  double res = (ranges + row_cost) * kIoNoCache;
  if (googlestats_log_level >= GsLogLevelLow) {
    printError("read_time for table %s index %d ranges %d and rows=%lld is %.2f\n",
               table->s->table_name.str, index, ranges, (long long int)rows, res);
  }
  return res;
}

int
ha_googlestats::extra(
  enum ha_extra_function operation)
{
  DBUG_ENTER("ha_googlestats::extra");
  DBUG_PRINT("enter", ("table %s, operation %d",
                       table->s->table_name.str, operation));
  switch (operation) {
    case HA_EXTRA_RESET_STATE:
      rowCount = 0;
      break;
    default:
      break;
  }
  DBUG_RETURN(0);
}

int ha_googlestats::reset()
{
  if (pushed_conds != 0) {
    pushed_conds->conds.clear();
  }
  return 0;
}

// Returns the string representation of the value of the key field.  'field'
// is a pointer to the Field.  The resulting string is stored and returned
// through 'val_str'.
inline
static void get_key_val_str(Field* field, String* val_str) {
  if (field->type() == MYSQL_TYPE_VARCHAR) {
    // VARCHAR keys are always stored with 2 byte length prefix.
    Field_varstring* varchar_field = (Field_varstring*)field;
    // length_bytes could be 1 or 2, so it must be saved to restore it later.
    uint32 old_length_bytes = varchar_field->length_bytes;
    varchar_field->length_bytes = 2;
    varchar_field->val_str(val_str, val_str);
    varchar_field->length_bytes = old_length_bytes;
  } else {
    field->val_str(val_str, val_str);
  }
}

ha_rows ha_googlestats::records_in_range(uint inx,
                                         key_range* min_key,
                                         key_range* max_key) {
  DBUG_ENTER("ha_googlestats::records_in_range");
  const char* kWho = "ha_googlestats::records_in_range";
  UpdateProcInfo updater(kWho);

  // Release latches before network activity.
  ha_release_temporary_latches(current_thd);

  KEY* key_info = &table->key_info[inx];

  // case "="
  if (min_key && max_key &&
      min_key->flag == HA_READ_KEY_EXACT &&
      max_key->flag == HA_READ_AFTER_KEY &&
      min_key->length == max_key->length &&
      memcmp(min_key->key, max_key->key, min_key->length) == 0)
  {
    if (get_sel_estimates()) {
      if (googlestats_log_level >= GsLogLevelLow) {
        printError("%s for table %s index %d get_sel_estimates failed\n",
                   kWho, table->s->table_name.str, inx);
      }
      DBUG_RETURN(HA_POS_ERROR);
    }

    uint num_key_parts = 0, key_offset = 0;
    while (num_key_parts <= key_info->user_defined_key_parts &&
           key_offset < min_key->length) {
      KEY_PART_INFO* part_info = &key_info->key_part[num_key_parts];
      key_offset += part_info->store_length;
      num_key_parts++;
    }
    ha_rows res = key_info->rec_per_key[num_key_parts - 1];
    if (googlestats_log_level >= GsLogLevelLow) {
      printError("%s for table %s index %d is %lld\n",
                 kWho, table->s->table_name.str, inx, (long long int)res);
    }
    DBUG_RETURN(res);
  } else {
    std::string request;
    request.append("COUNT db=");
    request.append(table->s->db.str);
    request.append(" table=");
    request.append(table->s->table_name.str);
    request.append(" index=");
    request.append(key_info->name);

    if (min_key) {
      request.append(" start_key=");
      for (uint i = 0, key_offset = 0; key_offset < min_key->length; ++i) {
        KEY_PART_INFO* part_info = &key_info->key_part[i];
        Field* field = part_info->field;
        memcpy(field->ptr, min_key->key + key_offset, part_info->store_length);
        if (i > 0) request.append(",");
        if (field->is_null()) {
          StatsServerAux::EscapeString("NULL", 4, &request);
        } else {
          String tmp;
          get_key_val_str(field, &tmp);
          StatsServerAux::EscapeString(tmp.c_ptr_safe(), tmp.length(), &request);
        }
        key_offset += part_info->store_length;
      }

      request.append(" start_range=");
      switch (min_key->flag) {
      case HA_READ_AFTER_KEY:
        request.append("gt");
        break;
      case HA_READ_KEY_EXACT:
        request.append("ge");
        break;
      default:
        printError("%s: unsupported range type for min key (%d)", kWho, min_key->flag);
        DBUG_RETURN(HA_POS_ERROR);
      }
    }

    if (max_key) {
      request.append(" end_key=");
      for (int i = 0, key_offset = 0; key_offset < (int)max_key->length; ++i) {
        KEY_PART_INFO* part_info = &key_info->key_part[i];
        Field* field = part_info->field;
        memcpy(field->ptr, max_key->key + key_offset, part_info->store_length);
        if (i > 0) request.append(",");
        if (field->is_null()) {
          StatsServerAux::EscapeString("NULL", 4, &request);
        } else {
          String tmp;
          get_key_val_str(field, &tmp);
          StatsServerAux::EscapeString(tmp.c_ptr_safe(), tmp.length(), &request);
        }
        key_offset += part_info->store_length;
      }

      request.append(" end_range=");
      switch (max_key->flag) {
      case HA_READ_AFTER_KEY:
        request.append("le");
        break;
      case HA_READ_BEFORE_KEY:
        request.append("lt");
        break;
      default:
        printError("%s: unsupported range type for max key (%d)", kWho, max_key->flag);
        DBUG_RETURN(HA_POS_ERROR);
      }
    }

    // Append user name.
    const char* auth_user = current_thd->main_security_ctx.user;
    if (auth_user != NULL) {
      int auth_user_len = strlen(auth_user);
      if (auth_user_len) {
        request.append(" user=");
        StatsServerAux::EscapeString(auth_user, auth_user_len, &request);
      }
    }

    request.append(" output=mysql3\n");
    if (googlestats_log_level >= GsLogLevelLow)
      printError("%s: %s", kWho, request.c_str());
    last_request = request;

    StatsServerConnectState state;

    while (!state.getIsDone()) {
      // Make sure we have a connection; we might get called before
      // index_/rnd_init().
      if (connect_server_with_state(&state, key_info->name)) {
        if (googlestats_log_level >= GsLogLevelLow) {
          printError("%s for table %s index %d connect_server failed\n",
                     kWho, table->s->table_name.str, inx);
        }

        continue;
      }

      int len = StatsServerAux::Write(sock_fd, request.c_str(), request.size());
      if (len != (int)request.length()) {
        printError("%s: error(%d) on write returning %d", kWho, errno, len);
        continue;
      }

      IncrementNonfetch timer(this);  // Time network IO wait

      // Read signature, which should be "SC01".

      char signature[4];
      if (StatsServerAux::readBytes(sock_fd, server_name, &signature,
                                    sizeof(signature), googlestats_timeout)) {
        printError("%s: signature didn't receive expected number of bytes", kWho);
        StatsServerCache::markTableBadAtServer(table->s->db.str,
                                               table->s->table_name.str,
                                               key_info->name, server_name,
                                               server_port);
        continue;
      }
      if (strncmp(signature, "SC01", sizeof(signature)) != 0) {
        printError("%s: received error from stats server (%c%c%c%c)",
                   kWho, signature[0], signature[1], signature[2], signature[3]);
        StatsServerCache::markTableBadAtServer(table->s->db.str,
                                               table->s->table_name.str,
                                               key_info->name, server_name,
                                               server_port);
        continue;
      }

      // Read number of rows.
      longlong num_rows;
      if (StatsServerAux::readBytes(sock_fd, server_name, &num_rows,
                                    INT_8_BYTES, googlestats_timeout)) {
        printError("%s: num_rows didn't receive expected number of bytes", kWho);
        StatsServerCache::markTableBadAtServer(table->s->db.str,
                                               table->s->table_name.str,
                                               key_info->name, server_name,
                                               server_port);
        continue;
      } else {
        // TODO(mcallaghan): Consider using 2 as the minimum result.
        ha_rows res = gs_max((longlong) 1, num_rows);
        if (googlestats_log_level >= GsLogLevelLow) {
          printError("records_in_range for table %s index %d is %lld\n",
                     table->s->table_name.str, inx, (long long int)res);
        }
        cleanup();
        DBUG_RETURN(res);
      }
    }
  }

  cleanup();

  // We should never fall-through here.
  DBUG_ASSERT(1);
  DBUG_RETURN(HA_POS_ERROR);
}

char*
ha_googlestats::active_index_name()
{
  if (active_index >= table->s->keys)
    return (char*) 0;
  else
    return table->key_info[active_index].name;
}

int
ha_googlestats::server_scan_init()
{
  int res = 0;

  // Release latches before network activity.
  ha_release_temporary_latches(current_thd);

  // Close the socket now. If this returns -1 on error, the socket
  // must be closed because MySQL ignores the return value (sometimes)
  // from rnd_init and index_init and if sock_fd != -1 there, they will
  // try to query a stats server.
  cleanup();

  // Read version number from db, cache it for duration of query. If there
  // isn't one then an appropriate error will be displayed elsewhere.
  if ((res = get_version_num()))
    return res;

  bool ok;
  schema_checksum = compute_schema_checksum(&ok);
  if (!ok)
    return GSS_ERR_COMPUTE_CHECKSUM;

  num_fetch_requests = 0;

  // Find a server we want to use for the duration of the query.
  return connect_server(active_index_name());
}

int
ha_googlestats::index_init(uint idx, bool sorted)
{
  const char* kWho = "ha_googlestats::index_init";
  UpdateProcInfo updater(kWho);
  if (googlestats_log_level >= GsLogLevelHigh)
    printError("%s: for %s", kWho, table->s->table_name.str);

  if (idx >= table->s->keys) {
    // There is something wrong with this table, since MySQL doesn't
    // know about this index's existence.
    printError("%s: selected index doesn't exist", kWho);
    return GSS_ERR_INDEX_INIT;
  }
  active_index = idx;

  return server_scan_init();
}

int
ha_googlestats::index_end()
{
  const char* kWho = "ha_googlestats::index_end";
  UpdateProcInfo updater(kWho);
  if (googlestats_log_level >= GsLogLevelHigh)
    printError("%s: for %s", kWho, table->s->table_name.str);

  num_buffered_rows = 0;
  cleanup();
  rows_left_to_fetch = false;
  active_index = MAX_KEY;
  return 0;
}

int
ha_googlestats::connect_server(const char* index_name)
{
  StatsServerConnectState state;
  return connect_server_with_state(&state, index_name);
}

int
ha_googlestats::connect_server_with_state(StatsServerConnectState* state,
                                          const char* index_name)
{
  const char* kWho = "ha_googlestats::connect_server";
  cleanup();

  std::string name;
  int error;
  sock_fd = StatsServerCache::connect(table->s->db.str,
                                      table->s->table_name.str,
                                      index_name, &name, &server_port, state,
                                      &error);
  server_tier = state->getTier();
  if (sock_fd < 0)
  {
    printError("%s: cannot get connection", kWho);
    last_connection_error = error;
    return(error);
  }
  rows_left_to_fetch = false;

  // Save server name and port, in case we need to mark it dead later.
  if (server_name != 0) {
    delete [] server_name;
  }
  server_name = new char[name.size() + 1];
  if (server_name == NULL) {
    printError("%s: cannot allocate %d bytes", kWho, (int)name.size());
    cleanup();
    return GSS_ERR_MEMORY;
  }

  strcpy(server_name, name.c_str());

  if (server_tier >= googlestats_remote_tier) {
    // Log a message when connecting to a "remote" tier.
    StatsServerAux::printError("connecting to a remote tier. tier(%d) "
                               "table(%s.%s.%s) host(%s) user(%s)",
                               server_tier,
                               table->s->db.str,
                               table->s->table_name.str,
                               index_name,
                               server_name,
                               current_thd->main_security_ctx.user);
  }

  return(0);
}


// Get selectivity estimate for single index, including per-column estimates.
// Assumes we have a server connection.
// Return the total number of rows.
longlong
ha_googlestats::get_sel_estimate(
  StatsServerConnectState *state,
  KEY* key_info)
{
  const char* kWho = "ha_googlestats::get_sel_estimate";
  std::string request;

  if (connect_server_with_state(state, key_info->name)) {
    printError("%s: cannot connect for %s", kWho, key_info->name);
    return -1;
  }

  request.append("SHOW_TABLE_INFO db=");
  request.append(table->s->db.str);
  request.append(" table=");
  request.append(table->s->table_name.str);
  request.append(" index=");
  request.append(key_info->name);
  request.append(" output=mysql3\n");
  if (googlestats_log_level >= GsLogLevelLow) {
    printError("%s: %s", kWho, request.c_str());
  }
  last_request = request;
  int len = StatsServerAux::Write(sock_fd, request.c_str(), request.size());
  if (len != (int) request.size()) {
    printError("%s: error on write", kWho);
    cleanup();
    return -1;
  }

  IncrementNonfetch timer(this);  // Time network IO wait

  longlong row_count;
  int32 num_est;
  // Read signature, which should be "SI01". On error, a 4-byte error
  // code will be returned instead, such as "E01\n".
  char signature[4];
  if (StatsServerAux::readBytes(sock_fd, server_name, signature,
                                sizeof(signature), googlestats_timeout)) {
    printError("%s: cannot read SHOW_TABLE_INFO response signature", kWho);
    StatsServerCache::markTableBadAtServer(table->s->db.str,
                                           table->s->table_name.str,
                                           key_info->name, server_name,
                                           server_port);
    return -1;
  }
  if (strncmp(signature, "SI01", 4) != 0) {
    printError("%s: received error from stats server (%c%c%c%c)",
               kWho, signature[0], signature[1], signature[2], signature[3]);
    StatsServerCache::markTableBadAtServer(table->s->db.str,
                                           table->s->table_name.str,
                                           key_info->name, server_name,
                                           server_port);
    return -1;
  }

  // Read row_count and num_est.
  uchar read_buf[INT_8_BYTES + INT_4_BYTES];
  if (StatsServerAux::readBytes(sock_fd, server_name, read_buf,
                                sizeof(read_buf), googlestats_timeout)) {
    printError("%s: cannot read SHOW_TABLE_INFO response", kWho);
    StatsServerCache::markTableBadAtServer(table->s->db.str,
                                           table->s->table_name.str,
                                           key_info->name, server_name,
                                           server_port);
    return -1;
  }

  // Get the number of rows in the packet.
  memcpy_field((uchar*) &row_count, read_buf, INT_8_BYTES);

  // Get the number of selectivity estimates.
  memcpy_field((uchar*) &num_est, read_buf + INT_8_BYTES, INT_4_BYTES);
  if (num_est <= 0 || num_est > 10000) {
    printError("%s: Bad index column count %d", kWho, num_est);
    return -1;
  }

  // Read selectivity estimates, one per index field.
  int32* est_buf = (int32*) my_malloc(num_est * sizeof(int32), MYF(0));
  if (StatsServerAux::readBytes(sock_fd, server_name, est_buf,
                                num_est * INT_4_BYTES,
                                googlestats_timeout)) {
    printError("%s: cannot read per field estimates", kWho);
    StatsServerCache::markTableBadAtServer(table->s->db.str,
                                           table->s->table_name.str,
                                           key_info->name, server_name,
                                           server_port);
    my_free(est_buf);
    return -1;
  }

  // Store it when it is within the range of key fields for the index.
  uint32 parts = gs_min((uint32) num_est, key_info->user_defined_key_parts);
  for (uint i = 0; i < parts; ++i)
    key_info->rec_per_key[i] = est_buf[i];

  // During some unusual schema changes in stats server, we might see
  // inconsistency here (where num_est < key_info->user_defined_key_parts).
  // This should be infrequent, so the table cardinality is used on the
  // assumption that each value is distinct.
  //
  // For secondary indexes, num_est will be > key_info->user_defined_key_parts
  // when the secondary index is not a superset of the primary key index.
  // The stats server adds fields to the secondary index to make it a
  // superset and returns estimates for the added fields. MySQL does not
  // know of the existence of those added fields.
  if (num_est < (int32) key_info->user_defined_key_parts) {
    printError("%s: Index metadata mismatch: num_est(%d), user_defined_key_parts(%d)",
               kWho, num_est, key_info->user_defined_key_parts);
    for (uint idx = num_est; idx < key_info->user_defined_key_parts; ++idx)
      key_info->rec_per_key[idx] = 1;
  }

  my_free(est_buf);
  return row_count;
}


// Set rows_in_tbl field.
int
ha_googlestats::get_sel_estimates()
{
  const char* kWho = "ha_googlestats::get_sel_estimates";
  longlong row_count_max = 0;

  if (googlestats_log_level >= GsLogLevelLow) {
    printError("%s: request selectivity estimates for %s",
               kWho, table->s->table_name.str);
  }

  // Record the current time.
  time_t current_time = time(NULL);
  double time_difference = difftime(current_time, sel_est_last_time);

  // Do not re-estimate under the following conditions:
  //   * The estimate has been done; and
  //   * The estimate is not too out-of-date.
  if (sel_est_last_time != -1 &&
      (time_difference < googlestats_estimate_interval)) {
    // We've already done this, and it is recent enough.
    return(0);
  }

  // Get selectivity estimates for each index.
  for (uint j = 0; j < table->s->keys; ++j) {
    KEY* key_info = &table->key_info[j];
    StatsServerConnectState state;
    int attempts;
    longlong row_count;

    for (attempts = 0; !state.getIsDone(); ++attempts) {
      row_count = get_sel_estimate(&state, key_info);
      if (row_count < 0) {
        if (googlestats_log_level >= GsLogLevelLow) {
          printError("%s: row_count < 0 for %s index %d (attempt %d)",
                     kWho, table->s->table_name.str, j, attempts+1);
        }
      } else {
        break;
      }
    }

    // From get_sel_estimate().
    cleanup();

    if (row_count < 0) {
      printError("%s: row_count < 0 for %s index %d after %d attempts",
                 kWho, table->s->table_name.str, j, attempts);
      return(-1);
    }

    // If this is the PK index, use the estimate from it. Otherwise, use the
    // largest estimate for number of rows in the table.
    if (j == table->s->primary_key || table->s->primary_key >= table->s->keys)
      row_count_max = gs_max(row_count_max, row_count);
  }

  if (googlestats_log_level >= GsLogLevelLow) {
    printError("%s: set row_count to %lld for %s\n",
               kWho, (long long int)row_count_max, table->s->table_name.str);
  }
  this->rows_in_tbl = row_count_max;

  // Remember the time for this estimate.
  sel_est_last_time = current_time;

  return(0);
}


int32
ha_googlestats::compute_schema_checksum(bool* ok)
{
  const char* kWho = "ha_googlestats::compute_schema_checksum";

  // Checksum of column types.
  *ok = true;
  int32 checksum = lzo_adler32(0, 0, 0);
  for (uint i = 0; i < table->s->fields; ++i) {
    Field* field = table->field[i];
    String sql_type;
    if (sql_type.alloc(128)) {
      printError("%s: malloc failed", kWho);
      *ok = false;
      return 0;
    }
    field->sql_type(sql_type);

    checksum = lzo_adler32(checksum, (lzo_byte*)field->field_name,
      strlen(field->field_name));
    checksum = lzo_adler32(checksum, (lzo_byte*)sql_type.ptr(), sql_type.length());
  }

  // Checksum of current index name and its column names.
  KEY* key_info = &table->key_info[active_index];
  std::string indexName(key_info->name);
  checksum = lzo_adler32(checksum, (lzo_byte*)key_info->name, strlen(key_info->name));
  for (uint j = 0; j < key_info->user_defined_key_parts; ++j) {
    Field* field = key_info->key_part[j].field;
    checksum = lzo_adler32(checksum, (lzo_byte*)field->field_name, strlen(field->field_name));
  }

  return(abs(checksum));
}

int
ha_googlestats::get_version_num()
{
  DBUG_ENTER("ha_googlestats::get_version_num");
  DBUG_PRINT("enter", ("table %s", table->s->table_name.str));
  const char* kWho = "ha_googlestats::get_version_num";

  if (table->googlestats_version_query_id != current_thd->query_id) {
    printError("%s: version number not set for %s", kWho, table->s->table_name.str);
    DBUG_RETURN(GSS_ERR_BAD_VERSION_NUM);
  }

  version_num = table->googlestats_version_num;
  version_hash = table->googlestats_version_hash;
  if (googlestats_log_level >= GsLogLevelHigh) {
    printError("%s: using version %d and hash %lld for table %s",
               kWho, version_num, (long long int)version_hash,
               table->s->table_name.str);
  }

  if (version_num < 0) {
    printError("%s: Bad version %d for table %s",
               kWho, version_num, table->s->table_name.str);
    DBUG_RETURN(GSS_ERR_BAD_VERSION_NUM);
  }

  DBUG_RETURN(0);
}

int
ha_googlestats::fill_row_buffer()
{
  const char* kWho = "ha_googlestats::fill_row_buffer";
  int res = 0;
  int last_res = 0;

  // Release latches before network activity.
  ha_release_temporary_latches(current_thd);

  // Loop until all servers have been considered at most twice.
  StatsServerConnectState state;
  int attempts;
  for (attempts = 0; !state.getIsDone(); ++attempts) {
    // Restart must be done if this is not the first attempt to fetch
    // the next batch of rows.
    if (attempts > 0) {
      if (scan_received_rows && !restart_bytes_used) {
        // Restart is not possible because there is no restart key.
        printError("%s: restart not supported", kWho);
        return GSS_ERR_NO_RESTART_KEY;
      }

      // Connect to another server.
      if ((res = connect_server_with_state(&state, active_index_name()))) {
        printError("%s: cannot connect to server", kWho);
        // Return previous error, which could be more informative to the user.
        return last_res;
      }

      // Restart the fetch using the restart key.
      if ((res = server_scan_restart())) {
        printError("%s: scan restart failed", kWho);
        // Return previous error, which could be more informative to the user.
        return last_res;
      }
    }

    // Request and read the next batch of rows.
    res = _fill_row_buffer();
    last_res = res;
    if (res == 0 || res == HA_ERR_END_OF_FILE) {
      // Either read the batch or at EOF.
      return res;
    }
    DBUG_ASSERT(res == -1 || (res >= GSS_ERR_FIRST && res <= GSS_ERR_LAST));

    // Unable to read the batch. Mark the table bad for the server.
    StatsServerCache::markTableBadAtServer(table->s->db.str,
                                           table->s->table_name.str,
                                           active_index_name(), server_name,
                                           server_port);
  }

  printError("%s: couldn't find server after %d tries", kWho, attempts);
  return res;
}

int
ha_googlestats::read_version_3_header(int32* packed_length,
                                      int32* unpacked_length,
                                      int32* num_rows,
                                      int32* bytes_for_rows)
{
  const char* kWho = "ha_googlestats::read_version_3_header";
  int res = 0;

  // Read signature, which should be "SS30". On error, a 4-byte error code
  // will be returned instead, such as "E01\n".
  char signature[4];
  if ((res = StatsServerAux::readBytes(sock_fd, server_name, signature,
                                      sizeof(signature), googlestats_timeout))) {
    printError("%s: Cannot read fetch response header(%d)",
               kWho, (int)sizeof(signature));
    return res;
  }
  if (strncmp(signature, "SS30", 4) != 0) {
    printError("%s: Did not receive correct signature (SS30), got (%c%c%c%c)",
               kWho, signature[0], signature[1], signature[2], signature[3]);
    return GSS_ERR_BAD_SIGNATURE;
  }

  uchar read_buf[INT_4_BYTES +   // packed_length
                 INT_4_BYTES +   // unpacked_length
                 INT_4_BYTES +   // num_rows
                 INT_4_BYTES];   // bytes_for_rows
  if ((res = StatsServerAux::readBytes(sock_fd, server_name, read_buf,
                                      sizeof(read_buf), googlestats_timeout))) {
    printError("%s: Cannot read fetch response header(%d)",
               kWho, (int)sizeof(read_buf));
    return res;
  }
  const uchar* offset = read_buf;

  // Get length of data when compressed.
  memcpy_field((uchar*) packed_length, offset, INT_4_BYTES);
  offset += INT_4_BYTES;

  // Get length of data when uncompressed.
  memcpy_field((uchar*) unpacked_length, offset, INT_4_BYTES);
  offset += INT_4_BYTES;

  // Get number of rows.
  memcpy_field((uchar*) num_rows, offset, INT_4_BYTES);
  offset += INT_4_BYTES;

  // Get number of bytes used for row data. Remaining bytes are used for
  // the fetch restart key.
  memcpy_field((uchar*) bytes_for_rows, offset, INT_4_BYTES);

  // Perform sanity checking on header values.
  if (*packed_length < 0 ||
      *unpacked_length < 0 ||
      *num_rows < 0 ||
      *bytes_for_rows < 0) {
    printError("%s: Invalid values packed_length(%d), unpacked_length(%d), "
               "num_rows(%d), row_bytes(%d)", kWho,
               *packed_length, *unpacked_length, *num_rows, *bytes_for_rows);
    return GSS_ERR_BAD_HEADER_VALUES;
  }
  if (*bytes_for_rows > *unpacked_length) {
    printError("%s: row_bytes(%d) must be <= unpacked_length(%d)",
               kWho, *bytes_for_rows, *unpacked_length);
    return GSS_ERR_BAD_HEADER_VALUES;
  }
  if (*packed_length > googlestats_max_packet_size ||
      *unpacked_length > googlestats_max_packet_size) {
    // TODO: Close the socket because data from it will not be read.
    printError("%s: packet is too large, max(%d), packed(%d), unpacked(%d)",
               kWho, googlestats_max_packet_size, *packed_length,
               *unpacked_length);
    return GSS_ERR_BAD_HEADER_VALUES;
  }

  if (googlestats_log_level >= GsLogLevelHigh) {
    printError("%s: packed_length(%d), unpacked_length(%d), num_rows(%d), "
               "row_bytes(%d)", kWho,
               *packed_length, *unpacked_length, *num_rows, *bytes_for_rows);
  }
  return 0;
}

int
ha_googlestats::read_version_3_data(int packed_length,
                                    int unpacked_length,
                                    int num_rows)
{
  const char* kWho = "ha_googlestats::read_version_3_data";
  int res = 0;

  // Read compressed data.
  if ((res = StatsServerAux::readBytes(sock_fd, server_name, data_buffer,
                                      packed_length, googlestats_timeout))) {
    // It looks like the server sent junk.
    printError("%s: Read response data failed - #compressed=%d, "
               "#uncompressed=%d, #rows=%d",
               kWho, packed_length, unpacked_length, num_rows);
    DBUG_ASSERT(0);
    return res;
  }

  // Decompress data using LZO.
  //
  // The typedef of lzo_uint is changed from unsigned int to unsigned long
  // from lzo-1.08 to lzo-2.02, so we should not depend on the that part to
  // get this right.
  lzo_uint num_data_bytes = requested_row_buffer_size;
  res = lzo1x_decompress_safe((lzo_byte*)data_buffer, packed_length,
                              (lzo_byte*)requested_row_buffer,
                              &num_data_bytes, 0);
  if (res != LZO_E_OK) {
    printError("%s: decompress %d bytes failed with result %d",
               kWho, packed_length, res);
    return GSS_ERR_DECOMPRESS_ERR;
  }

  if ((int)num_data_bytes != unpacked_length) {
    printError("%s: decompression produced %d bytes and %d were expected ",
               kWho, (int)num_data_bytes, unpacked_length);
    DBUG_ASSERT(0);
    return GSS_ERR_DECOMPRESS_ERR;
  }
  return 0;
}

int
ha_googlestats::_fill_row_buffer()
{
  const char* kWho = "ha_googlestats::_fill_row_buffer";
  int res = 0;

  if (num_received_rows >= num_requested_rows) {
    if ((res = server_scan_next())) {
      printError("%s: server_scan_next failed", kWho);
      return(res);
    }
  }

  if (requested_row_buffer != 0 && max_buffered_rows != 0) {
    // Before doing anything that could result in an error
    // and make us restart the scan, save the restart row;
    // we'd need that for the restart. This is the restart
    // key from the last packet received which can then be
    // used to restart a fetch to get the next packet that
    // is about to be received.
    //
    // This saves the restart key from the packet has just
    // been consumed. The check for max_buffered_rows != 0
    // prevents this from being called before fetching the
    // first packet.
    if ((res = get_restart_row())) {
      printError("%s: cannot get restart row", kWho);
      return res;
    }
  }

  IncrementFetch timer(this);  // Time network IO wait

  int32 packed_length;   // Length of data when compressed
  int32 unpacked_length; // Length of data when uncompressed
  int32 num_rows;        // Number of rows in compressed data

  if ((res = read_version_3_header(&packed_length, &unpacked_length, &num_rows,
                                  &row_bytes))) {
    printError("%s: cannot read response header", kWho);
    return res;
  }

  if (num_rows == 0) {
    rows_left_to_fetch = false;
    return(HA_ERR_END_OF_FILE);
  }

  // Make the buffer for compressed data large enough.
  if (data_buffer_size < packed_length) {
    delete [] data_buffer;
    data_buffer_size = packed_length;
    data_buffer = new uchar[data_buffer_size];
    if (data_buffer == 0) {
      data_buffer_size = 0;
      printError("%s: couldn't allocate data buffer for %d bytes",
                 kWho, data_buffer_size);
      return GSS_ERR_MEMORY;
    }
  }

  // Make the buffer for uncompressed data large enough. See ::get_buffer_row
  // for how the column values are transferred to the output row.
  if (requested_row_buffer_size < unpacked_length) {
    delete [] requested_row_buffer;
    requested_row_buffer_size = unpacked_length;
    requested_row_buffer = new uchar[requested_row_buffer_size];
    if (requested_row_buffer == 0) {
      requested_row_buffer_size = 0;
      printError("%s: couldn't allocate decompressed row buffer for %d bytes",
                 kWho, requested_row_buffer_size);
      return GSS_ERR_MEMORY;
    }
  }

  if ((res = read_version_3_data(packed_length, unpacked_length, num_rows))) {
    printError("%s: cannot read version 3 packet", kWho);
    return res;
  }
  if (fixed_length_row) {
    // No need to use the slower general version 3 copier.
    get_requested_row = &ha_googlestats::get_fixed_row;
  } else {
    get_requested_row = &ha_googlestats::get_variable_row;
  }

  // Determine the size of the restart key. It may be 0 (no key).

  int restart_length = unpacked_length - row_bytes;
  restart_bytes_used = 0;
  if (googlestats_log_level >= GsLogLevelHigh)
    printError("%s: %d restart key length", kWho, restart_length);
  if (restart_length) {
    // The first 4 bytes of the restart key has the number of bytes
    // that follow. This must be equal to restart_length - 4.
    // If a binary restart key is used, it must be restart_length - 8.
    int32 bytes_used;
    binary_restart_key = false;
    memcpy_field((uchar*)&bytes_used, (uchar*)(requested_row_buffer + row_bytes),
                 INT_4_BYTES);

    if (bytes_used == (restart_length - INT_8_BYTES)) {
      // This is a binary restart key.
      binary_restart_key = true;
      // Change to the true number of bytes used for the binary restart key.
      bytes_used = bytes_used + INT_4_BYTES;
    }

    if (bytes_used != (restart_length - INT_4_BYTES)) {
      printError("%s: Length in restart key is %d but %d is expected",
                 kWho, bytes_used, (restart_length - INT_4_BYTES));
      return GSS_ERR_BAD_RESTART_KEY;
    }
    restart_bytes_used = bytes_used;
  }

  timer.set_rows_and_bytes(num_rows, packed_length);

  rows_read += num_rows;

  max_buffered_rows = num_rows;
  num_buffered_rows = max_buffered_rows;
  num_received_rows += num_buffered_rows;
  scan_received_rows = true;

  if (num_received_rows >= num_requested_rows) {
    if (googlestats_log_level >= GsLogLevelMed)
      printError("%s: received all rows from fetch command", kWho);
    rows_left_to_fetch = false;
  }

  // Reset 'next' row to point at the first requested row.
  next_requested_row = requested_row_buffer;

  return(0);
}

int
ha_googlestats::get_buffer_row(uchar* buf)
{
  const char* kWho = "ha_googlestats::get_buffer_row";
  if (num_buffered_rows == 0) {
    int res = fill_row_buffer();
    if (res == HA_ERR_END_OF_FILE) {
      return HA_ERR_END_OF_FILE;
    }
    if (res != 0) {
      printError("%s: cannot get more rows", kWho);
      return res;
    }
  }

  if (next_requested_row >=
      (requested_row_buffer + requested_row_buffer_size)) {
    printError("%s: Exceeded end of row buffer during fetch", kWho);
    return GSS_ERR_GET_ROW;
  }

  // Copy next row and advance pointer.
  int bytes_used = (this->*get_requested_row)(buf, next_requested_row);
  if (bytes_used < 0) {
    printError("%s: cannot get another row", kWho);
    return GSS_ERR_GET_ROW;
  }
  next_requested_row += bytes_used;
  --num_buffered_rows;
  return 0;
}

int
ha_googlestats::server_scan_next()
{
  const char* kWho = "ha_googlestats::server_scan_next";
  std::string request("FETCH_MORE num_rows=");
  char numstr[21];
  num_requested_rows = gs_min(num_requested_rows * 2, MAX_NUM_ROWS_TO_FETCH);
  sprintf(numstr, "%d", num_requested_rows);
  request.append(numstr);
  request.append("\n");
  if (googlestats_log_level >= GsLogLevelMed) {
    printError("%s: %s", kWho, request.c_str());
  }

  // Don't check for timeouts on write, we'll do it on the subsequent read.
  last_request = request;
  int len = StatsServerAux::Write(sock_fd, request.c_str(), request.size());
  if (len != (int) request.size()) {
    statistic_increment(google_network_io_failures, &LOCK_stats);
    printError("%s: error on write", kWho);
    return(GSS_ERR_SOCKET_WRITE);
  }
  num_received_rows = 0;
  rows_left_to_fetch = true;
  return(0);
}

// Start new scan with saved parameters.
int
ha_googlestats::server_scan_restart()
{
  const char* kWho = "ha_googlestats::server_scan_restart";
  keys_index = keys_index_for_restart;
  if (!scan_received_rows) {
    // We haven't received any rows from failed server, start
    // completely from scratch.
    num_requested_keys = 1;
    return(server_scan_start(0));
  }

  // XXX: pick up fields belonging to active index and
  // primary index in single scan through table->field by
  // examining Field::part_of_key

  if (table->s->primary_key >= table->s->keys) {
    // It doesn't look like this table has a primary key; bail now,
    // we need a primary key to restart.
    printError("%s: no primary key", kWho);
    return(GSS_ERR_INDEX_INIT);
  }

  if (restart_bytes_used == 0) {
    // The stats server does not have to return a restart key.
    return GSS_ERR_NO_RESTART_KEY;
  }

  // Assemble key parts to print for the restart key.
  StatsServerKeyVals key_vals;
  if (!binary_restart_key &&
      get_restart_key_parts(&key_vals, table, active_index) != 0) {
    printError("%s: cannot get restart key", kWho);
    return GSS_ERR_BAD_RESTART_KEY;
  }

  return (server_scan_start(&key_vals));
}



int
ha_googlestats::server_scan_start(
  const uchar* key,
  uint key_len,
  enum ha_rkey_function find_flag,
  RequestType req_type)
{

  // Release latches before network activity.
  ha_release_temporary_latches(current_thd);

  // Extract lookup key values into orig_keys.
  if (orig_keys != 0) {
    delete orig_keys;
    orig_keys = 0;
  }

  if (key_len > 0) {
    // Make sure this only gets allocated if we have any key values.
    orig_keys = new StatsServerKeys();
    if (orig_keys == NULL) {
      printError("ha_googlestats::server_scan_start: insufficient memory");
      return(-1);
    }
    // We have key values for 1 key.
    orig_keys->vals.resize(1);
  }

  KEY* key_info = &table->key_info[active_index];
  const uchar* val_ptr = key;
  for (uint i = 0; i < key_info->user_defined_key_parts && val_ptr < key + key_len; ++i) {
    KEY_PART_INFO* part_info = &key_info->key_part[i];
    Field* field = part_info->field;

    // Figure out whether we're looking at a NULL value.
    bool is_null = false;
    if (field->real_maybe_null()) {
      is_null = (*val_ptr == 1);
      ++val_ptr;
    }
    if (is_null) {
      orig_keys->vals[0].push_back("NULL");
    } else {
      memcpy(field->ptr, val_ptr, part_info->store_length);
      String tmp;
      get_key_val_str(field, &tmp);
      orig_keys->vals[0].push_back(tmp.c_ptr_safe());
    }
    val_ptr += part_info->store_length;
  }
  orig_find_flag = find_flag;
  orig_request_type = req_type;
  scan_received_rows = false; // We haven't seen anything yet.

  keys_index = 0;
  num_requested_keys = 1;
  return(server_scan_start(0));
}


int
ha_googlestats::server_scan_start(
  StatsServerKeyVals* start_key_vals)
{
  const char* kWho = "ha_googlestats::server_scan_start";
  int res = 0;
  // Drain rows from a previous scan, if any.
  // TODO(mcallaghan): Is there a better way to do this?
  while (rows_left_to_fetch) {
    // HA_ERR_END_OF_FILE should NOT fail the draining.
    if ((res = fill_row_buffer()) && res != HA_ERR_END_OF_FILE) {
      printError("%s: drain failed from previous operation", kWho);
      return(res);
    }
  }

  if (version_num == -1) {
    printError("%s: version_num not set for fetch from %s",
               kWho, table->s->table_name.str);
    return GSS_ERR_BAD_VERSION_NUM;
  }

  ++num_fetch_requests;

  KEY* key_info = &table->key_info[active_index];

  num_requested_rows = MIN_NUM_ROWS_TO_FETCH;
  if (THDVAR(current_thd, initial_fetch_rows)) {
    num_requested_rows = THDVAR(current_thd, initial_fetch_rows);
  }
  // If we're specifying all key values for the primary key, we only expect to
  // get a single row back.  However, if it is a FetchMulti request, multiple
  // rows can be returned.
  if (strcmp(key_info->name, "PRIMARY") == 0
      && orig_keys != 0 && orig_keys->vals.size() == 1
      && orig_keys->vals[0].size() == key_info->user_defined_key_parts
      && orig_find_flag == HA_READ_KEY_EXACT
      && orig_request_type != FetchMulti)
  {
    num_requested_rows = 1;
  }

  // TODO: Send the new optional commands:
  //   'MAX_PACKET_SIZE=googlestats_max_packet_size'
  //   'LIMIT=X'
  std::string request;
  char numstr[21];
  if (orig_request_type == Fetch || orig_request_type == FetchMulti) {
    request.append("FETCH ");
  } else {
    request.append("FETCH_LAST ");
  }
  request.append("use_compression=1 ");
  if (orig_request_type == Fetch || orig_request_type == FetchMulti) {
    // FETCH_LAST must not pass a num_rows argument.
    request.append("num_rows=");
    sprintf(numstr, "%d", num_requested_rows);
    request.append(numstr);
  }
  request.append(" schema_hash=");
  sprintf(numstr, "%u", schema_checksum);
  request.append(numstr);
  request.append(" db=");
  request.append(table->s->db.str);
  request.append(" table=");
  request.append(table->s->table_name.str);
  request.append(" index=");
  request.append(key_info->name);
  request.append(" version=");
  sprintf(numstr, "%d", version_num);
  request.append(numstr);
  request.append(" version_hash=");
  sprintf(numstr, "%lld", (long long int)version_hash);
  request.append(numstr);

  std::string columns_request;
  if (request_columns(&columns_request) != 0) {
    printError("%s: request_columns fail", kWho);
    return(GSS_ERR_REQUEST_COLUMNS);
  }
  request.append(columns_request);

  if (orig_keys != 0) {
    // We have a key, put it in the request.
    request.append(" range=");
    switch (orig_find_flag) {
    case HA_READ_AFTER_KEY:
      request.append("gt");
      break;
    case HA_READ_KEY_EXACT:
      request.append("eq");
      break;
    case HA_READ_KEY_OR_NEXT:
      request.append("ge");
      break;
    default:
      printError("%s: unsupported scan type (%d)", kWho, orig_find_flag);
      return(GSS_ERR_BAD_SCAN_TYPE);
    }

    uint max_keys = 0;
    if (THDVAR(current_thd, multifetch_max_keys)) {
      max_keys = THDVAR(current_thd, multifetch_max_keys);
      max_keys = gs_min(max_keys, num_requested_keys);
    } else {
      max_keys = num_requested_keys;
    }

    // Save the current key_index, for possibly restarting this FETCH later.
    keys_index_for_restart = keys_index;

    // A map for keys in the request, to find duplicates.
    std::map<std::string, int> existing_keys;

    uint index;
    for (index = keys_index; index < orig_keys->vals.size(); ++index) {
      std::vector<std::string>* key = &orig_keys->vals[index];
      if (index > keys_index && max_keys && (index - keys_index) >= max_keys) {
        break;
      }
      std::string key_string;
      for (std::vector<std::string>::iterator val = key->begin();
           val != key->end(); ++val)
      {
        if (val != key->begin()) {
          key_string.append(",");
        }
        StatsServerAux::EscapeString(val->c_str(), val->length(), &key_string);
      }

      // Save this key in the map.
      std::pair<std::map<std::string, int>::iterator, bool> ret_value;
      ret_value = existing_keys.insert(std::make_pair(key_string, 1));
      if (!ret_value.second) {
        // This key already exists in the request.  Stop adding keys to this
        // FETCH request.
        break;
      }

      // Add this key to the current FETCH request.
      request.append(" Keys={key=");
      request.append(key_string);
      request.append(" }");
    }

    // Save the actual number of requested keys.
    num_requested_keys = index - keys_index;

    // Save the index for the next scan start.
    keys_index = index;
  }

  if (start_key_vals != 0) {
    if (!binary_restart_key) {
      request.append(" resume_after_key=");
      for (std::vector<std::string>::iterator val =
               start_key_vals->vals.begin();
           val != start_key_vals->vals.end(); ++val)
      {
        if (val != start_key_vals->vals.begin()) {
          request.append(",");
        }
        StatsServerAux::EscapeString(val->c_str(), val->length(), &request);
      }
    } else {
      // Use binary restart key
      request.append(" binary_restart_key=");
      StatsServerAux::EscapeString((const char*)restart_row, restart_bytes_used, &request);
    }
  }

  // Appends condition pushdowns, if there are any.
  if ((orig_request_type == Fetch || orig_request_type == FetchMulti) &&
      pushed_conds != 0 &&
      !pushed_conds->conds.empty()) {
    for (std::vector<std::string>::iterator val = pushed_conds->conds.begin();
         val != pushed_conds->conds.end(); ++val) {
      if (val != pushed_conds->conds.begin()) {
        request.append(",");
      } else {
        request.append(" where=");
      }
      // 'val' was escaped earlier.
      request.append(*val);
    }
  }

  // Append user name.
  const char* auth_user = current_thd->main_security_ctx.user;
  if (auth_user != NULL) {
    int auth_user_len = strlen(auth_user);
    if (auth_user_len) {
      request.append(" user=");
      StatsServerAux::EscapeString(auth_user, auth_user_len, &request);
    }
  }

  request.append(" output=mysql4 \n");

  if (googlestats_log_level >= GsLogLevelLow) {
    printError("%s: %s", kWho, request.c_str());
  }

  // Don't check for timeouts on write, we'll do it on the subsequent read.
  last_fetch_request = request;
  last_request = request;
  int len = StatsServerAux::Write(sock_fd, request.c_str(), request.size());
  if (len != (int) request.size()) {
    printError("%s: error on write", kWho);
    return(GSS_ERR_SOCKET_WRITE);
  }
  num_buffered_rows = 0;
  num_received_rows = 0;
  rows_left_to_fetch = true;
  // Reset, so that we don't accidentally call get_restart_row on the
  // next request.
  max_buffered_rows = 0;
  return(0);
}

int
ha_googlestats::index_read(
  uchar * buf,
  const uchar * key,
  uint key_len,
  enum ha_rkey_function find_flag)
{
  const char* kWho = "ha_googlestats::index_read";
  UpdateProcInfo updater(kWho);
  int res = 0;

  if (googlestats_log_level >= GsLogLevelHigh) {
    printError("%s: for %s", kWho, table->s->table_name.str);
  }

  if (sock_fd < 0) {
    // init() didn't succeed
    printError("%s: no server connection", kWho);
    return(last_connection_error);
  }
  if ((res = server_scan_start(key, key_len, find_flag, Fetch))) {
    printError("%s: cannot start scan", kWho);
    return(res);
  }

  int status = index_next(buf);
  if (status == HA_ERR_END_OF_FILE) {
    // This is the start of the scan, we won't return anything at all.
    return(HA_ERR_KEY_NOT_FOUND);
  }
  return(status);
}

int
ha_googlestats::index_read_idx(
  uchar * buf,
  uint index,
  const uchar * key,
  uint key_len,
  enum ha_rkey_function find_flag)
{
  const char* kWho = "ha_googlestats::index_read_idx";
  UpdateProcInfo updater(kWho);
  int res = 0;

  if ((res = index_init(index, false))) {
    printError("%s: index_init failed", kWho);
    return res;
  } else {
    return index_read(buf, key, key_len, find_flag);
  }
}

int
ha_googlestats::index_next(uchar * buf)
{
  const char* kWho = "ha_googlestats::index_next";
  UpdateProcInfo updater(kWho);
  if (googlestats_log_level >= GsLogLevelHigh) {
    printError("%s: for %s", kWho, table->s->table_name.str);
  }

  if (sock_fd < 0) {
    printError("%s: no server connection", kWho);
    // init() didn't succeed
    return(last_connection_error);
  }
  int res = get_buffer_row(buf);

  if (res == HA_ERR_END_OF_FILE) {
    table->status = STATUS_NOT_FOUND;
    return(my_errno = HA_ERR_END_OF_FILE);
  }
  if (res != 0) {
    printError("%s: cannot get more rows", kWho);
    return(res);
  }
  table->status = 0;
  return(0);
}

int
ha_googlestats::index_first(uchar* buf)
{
  const char* kWho = "ha_googlestats::index_first";
  UpdateProcInfo updater(kWho);
  int res = 0;

  if (googlestats_log_level >= GsLogLevelHigh) {
    printError("%s: for %s", kWho, table->s->table_name.str);
  }

  if (sock_fd < 0) {
    printError("%s: no server connection", kWho);
    // init() didn't succeed
    return(last_connection_error);
  }
  if ((res = server_scan_start(0, 0, HA_READ_BEFORE_KEY, Fetch))) {
    printError("%s: cannot start server scan", kWho);
    return(res);
  }

  int status = index_next(buf);
  return(status);
}

int
ha_googlestats::index_last(uchar * buf)
{
  const char* kWho = "ha_googlestats::index_last";
  UpdateProcInfo updater(kWho);
  int res = 0;

  if (googlestats_log_level >= GsLogLevelHigh) {
    printError("%s: for %s", kWho, table->s->table_name.str);
  }

  if (sock_fd < 0) {
    // init() didn't succeed
    printError("%s: no server connection", kWho);
    return(last_connection_error);
  }
  if ((res = server_scan_start(0, 0, HA_READ_BEFORE_KEY, FetchLast))) {
    printError("%s: cannot start scan", kWho);
    return(res);
  }

  int status = index_next(buf);
  return(status);
}

int
ha_googlestats::index_next_same(uchar *buf, const uchar *key, uint keylen)
{
  const char* kWho = "ha_googlestats::index_next_name";
  UpdateProcInfo updater(kWho);

  if (googlestats_log_level >= GsLogLevelHigh)
    printError("%s: for %s", kWho, table->s->table_name.str);

  if (sock_fd < 0) {
    // init() didn't succeed
    printError("%s: no server connection", kWho);
    return last_connection_error;
  } else {
    return index_next(buf);
  }
}

int
ha_googlestats::rnd_init(
  bool scan)
{
  const char* kWho = "ha_googlestats::rnd_init";
  UpdateProcInfo updater(kWho);
  int res = 0;

  if (table->s->keys == 0) {
    // This table doesn't have a primary key, abort.
    printError("%s: table doesn't have primary key", kWho);
    return(GSS_ERR_INDEX_INIT);
  }
  active_index = 0; // Scan of primary key.

  if ((res = server_scan_init())) {
    printError("%s: cannot init server scan", kWho);
    return(res);
  }

  if ((res = server_scan_start(0, 0, HA_READ_AFTER_KEY, Fetch))) {
    printError("%s: cannot start server scan", kWho);
    return(res);
  }
  return(0);
}

int
ha_googlestats::rnd_next(uchar *buf)
{
  const char* kWho = "ha_googlestats::rnd_next";
  UpdateProcInfo updater(kWho);

  if (sock_fd < 0) {
    // init() didn't succeed
    printError("%s: no server connection", kWho);
    return(last_connection_error);
  }
  int res = get_buffer_row(buf);
  if (res == HA_ERR_END_OF_FILE) {
    table->status = STATUS_NOT_FOUND;
    return(my_errno = HA_ERR_END_OF_FILE);
  }
  if (res != 0) {
    printError("%s: cannot get rows", kWho);
    return(res);
  }
  table->status = 0;
  return(0);
}

int
ha_googlestats::rnd_end()
{
  const char* kWho = "ha_googlestats::rnd_end";
  UpdateProcInfo updater(kWho);

  num_buffered_rows = 0;
  cleanup();
  rows_left_to_fetch = false;

  return 0;  // ha_innodb::rnd_end() returns 0
}

THR_LOCK_DATA **
ha_googlestats::store_lock(
  THD *thd,
  THR_LOCK_DATA **to,
  enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) {
    lock.type = lock_type;
  }
  *to++= &lock;
  return to;
}

void
ha_googlestats::position(const uchar *record) {
  // TODO(mcallaghan): It would be nice to prevent this from being called, but
  // that does not appear to be possible. rnd_pos() always fails, so the fact
  // that nothing is done is here is probably not a problem. This exists in
  // the 4.0 handler interface and GoogleStats did nothing there as well.
  if (googlestats_log_level >= GsLogLevelLow)
    printError("ha_googlestats::position must not be called");
}

// external lock - called at beginning of query to lock, at end to unlock tables
//
// At unlock time, we use it to invalidate cached version number

int
ha_googlestats::external_lock(
  THD *thd,
  int lock_type)
{
  UpdateProcInfo updater("ha_googlestats:external_lock");
  if (lock_type == F_UNLCK) {
    // At statement end.
    version_num = -1;
    version_hash = -1;
    cleanup();
  } else {
    // At statement start.
    if (!THDVAR(current_thd, query_on_update) && might_write_binlog()) {
      switch (current_thd->lex->sql_command) {
        case SQLCOM_INSERT_SELECT:
        case SQLCOM_REPLACE_SELECT:
        case SQLCOM_UPDATE_MULTI:
        case SQLCOM_DELETE_MULTI:
          return (my_errno = HA_ERR_NO_QUERY_GOOGLESTATS_ON_UPDATE);
        case SQLCOM_CREATE_TABLE:
          if (current_thd->lex->query_tables) // Fail on CREATE TABLE ... SELECT.
            return (my_errno = HA_ERR_NO_QUERY_GOOGLESTATS_ON_UPDATE);
          break;
        default:
          break;
      }
    }
  }

  return 0;
}

int
ha_googlestats::classify_fields() {
  const char* kWho = "ha_googlestats::classify_fields";
  fixed_length_row = true; // Assume no variable length fields.

  // Determine whether all fetched fields are fixed length.
  for (uint i = 0; i < table->s->fields; ++i) {
    // TODO: Check field descriptor if its query_id is our query_id.
    Field* field = table->field[i];

    if (googlestats_log_level >= GsLogLevelHigh) {
      printError("field %s has type %d, real type %d, field length %d "
                 "pack length %d", field->field_name,
                 field->type(), field->real_type(), field->field_length,
                 field->pack_length());
    }

    if (bitmap_is_set(table->read_set, field->field_index)) {
      switch (field->type()) {
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_BLOB:
          fixed_length_row = false;
          break;
        case MYSQL_TYPE_VAR_STRING:
          // This type may be encountered for tables defined when MySQL4 was
          // used. The upgrade process requires that this table be dropped
          // and recreated. I don't want to support both types of internal
          // encoding for SQL varchar columns. The format is described at
          // http://dev.mysql.com/doc/internals/en/myisam-column-attributes.html
          printError("%s: field %s has type MYSQL_TYPE_VAR_STRING which is "
                     "not supported. Drop and recreate the table", kWho,
                     field->field_name);
          return -1;
        default:
          break;
      }
    }
  }
  return 0;
}

// Put names of all fields requested for this query (table->read_set bit
// for the field is set) into the 'field' option of the request string
// and set nullable_request_field_count, requested_row_length.
int
ha_googlestats::request_columns(std::string* columns_request)
{
  const char* kWho = "ha_googlestats::request_columns";
  last_query_id = current_thd->query_id;
  request_field_count = 0;
  columns_request->clear();

  // Allocate an array for the request fields if necessary.
  if (request_fields == 0) {
    request_field_max = table->s->fields;
    request_fields = new RequestField[request_field_max];
    if (request_fields == NULL) {
      printError("%s: cannot allocate %d bytes for fields",
                 kWho, (int)(sizeof(RequestField) * request_field_max));
      return -1;
    }
  }

  if (request_field_max < (int)table->s->fields) {
    printError("%s: request_field_max(%d) < table_share->fields(%d)",
               kWho, request_field_max, table->s->fields);
    DBUG_ASSERT(0);
    return -1;
  }

  RequestField* req_field = request_fields;
  uint null_bit = 1;
  uint null_offset = 0;

  // TODO: Previous code is still questioning if this is true. This seems to be
  // the correct way to get the row header length, but there's no definitive
  // documentation in MySQL regarding it.
  if (table->field[0]->ptr < table->record[0]) {
    printError("%s: field pointer (%p) precedes the record (%p) for %s", kWho,
               table->field[0]->ptr, table->record[0],
               table->field[0]->field_name);
    DBUG_ASSERT(0);
    return -1;
  }
  row_header_length = table->field[0]->ptr - table->record[0];

  // Determine if fields are all fixed length.
  if (classify_fields())
    return -1;

  columns_request->append(" fields=");
  nullable_request_field_count = 0;
  requested_row_length = 0;

  for (uint i = 0; i < table->s->fields; ++i) {
    // Check to see if table->read_set bitmap has the field bit set.
    Field* field = table->field[i];
    if (bitmap_is_set(table->read_set, field->field_index)) {
      // We need this column for the query.
      // Optimize for frequently used fields during row copying.
      req_field->pack_length = field->pack_length();
      req_field->field_offset = field->ptr - table->record[0];
      req_field->real_maybe_null = field->real_maybe_null();
      req_field->field = field;
      req_field->set_field_type(field->type());

      if (field->ptr < table->record[0]) {
        printError("%s: field pointer precedes the record for %s",
                   kWho, field->field_name);
        DBUG_ASSERT(0);
        return -1;
      }
      if (req_field->pack_length < 0 ||
          req_field->field_offset < 0 ||
          (req_field->pack_length + req_field->field_offset)
          > (int)stats.mean_rec_length) {
        printError("%s: pack length(%d) must be >= 0 and "
                   "field offset(%d) must be >= 0 and "
                   "field end (%d) must be within the limit (%lu) for %s ",
                   kWho, req_field->pack_length, req_field->field_offset,
                   (req_field->pack_length + req_field->field_offset),
                   stats.mean_rec_length, field->field_name);
        DBUG_ASSERT(0);
        return -1;
      }

      if (request_field_count > 0)
        columns_request->append(",");

      columns_request->append(field->field_name);
      if (req_field->real_maybe_null) {
        req_field->req_null_bit = null_bit;
        req_field->req_null_offset = null_offset;
        if ((null_bit <<= 1) == 256) {
          ++null_offset;
          null_bit = 1;
        }
        ++nullable_request_field_count;
      }

      requested_row_length += req_field->pack_length;
      ++request_field_count;
      ++req_field;
    }
  }
  columns_request->append(" ");

  // Header has one bit per requested nullable column.
  requested_row_header_length = (nullable_request_field_count + 7) / 8;
  requested_row_length += requested_row_header_length;

  return 0;
}

// Set the null bit in the null column bitmap at the start of out_row
// if the column described by req_field in row_start is null.
static inline void set_null_bit(RequestField* req_field,
                                const uchar* row_start,
                                const uchar* record_start,
                                uchar* out_row) {
  if (req_field->real_maybe_null) {
    if (row_start[req_field->req_null_offset] & req_field->req_null_bit) {
      int null_offset = (uchar*)req_field->field->null_ptr - record_start;
      out_row[null_offset] |= req_field->field->null_bit;
    }
  }
}

int
ha_googlestats::copy_varstring_field(RequestField* req_field,
                                     const uchar* row_start,
                                     int field_offset,
                                     uchar* out_row,
                                     int* bytes_written) {
  const char* kWho = "ha_googlestats::copy_varstring_field";
  ushort field_len;  // The length of the varchar column.
  const uchar* field_start = row_start + field_offset;
  shortget(field_len, field_start);

  // If the field length in the schema is > 255, MySQL expects 2 bytes for
  // the length of the data returned.
  int bytes_in_length = HA_VARCHAR_PACKLENGTH(req_field->field->field_length);

  // As of MySQL 5.*, VARCHARs are limited to 65535.
  if (req_field->field->field_length > MAX_FIELD_VARCHARLENGTH) {
    printError("%s: varchar field %s is invalid (%d)",
               kWho, req_field->field->field_name,
               req_field->field->field_length);
    return -1;
  }

  // Ensure that the data returned is less than the size of the field.
  if (field_len > req_field->field->field_length) {
    printError("%s: varchar field %s too much data (%d)",
               kWho, req_field->field->field_name, field_len);
    return -1;
  }

  // Update number of bytes written to the output row.
  *bytes_written += req_field->field->field_length;

  // Check for out-of-bounds writes.
  if (*bytes_written > (int)stats.mean_rec_length) {
    printError("%s: Output row written beyond end of buffer, write(%d) max(%lu)",
               kWho, *bytes_written, stats.mean_rec_length);
    return -1;
  }

  // Format for MYSQL_TYPE_VARCHAR is (len)(data):
  //   * data is not padded; and
  //   * len is:
  //     - 1 byte for varchar(255) or smaller; or
  //     - 2 bytes for varchar(256) or larger.

  // Set the length.
  int2store(out_row + req_field->field_offset, field_len);

  // Copy the data skipping the two length bytes in the input buffer.
  memcpy(out_row + req_field->field_offset + bytes_in_length, field_start + 2,
         field_len);

  set_null_bit(req_field, row_start, table->record[0], out_row);

  // Return number of bytes read.
  return (2 + field_len);
}

int
ha_googlestats::copy_blob_field(RequestField* req_field,
                                const uchar* row_start,
                                int field_offset,
                                uchar* out_row,
                                int* bytes_written) {
  const char* kWho = "ha_googlestats::copy_blob_field";
  Field_blob* blob_field = (Field_blob*)req_field->field;
  // Number of bytes of BLOB data.
  uint field_len;
  // The length of the length field for the BLOB.
  int blob_length_length = blob_field->pack_length() - portable_sizeof_char_ptr;
  const uchar* field_start = row_start + field_offset;

  switch (blob_length_length) {
    case 1:
      field_len = (uchar) *field_start;
      break;
    case 2:
      {
        ushort tmp;
        shortget(tmp, field_start);
        field_len = tmp;
      }
      break;
    case 3:
      field_len = uint3korr(field_start);
      break;
    case 4:
      longget(field_len, field_start);
      break;
    default:
      printError("%s: Invalid blob length(%d)", kWho, blob_length_length);
      DBUG_ASSERT(0);
      return -1;
  }

  // Check for out-of-bounds reads.
  if ((field_len + row_start) > (requested_row_buffer + row_bytes)) {
    printError("%s: Blob field is too large(%d)", kWho, field_len);
    return -1;
  }

  // Copy the BLOB length into the output row.
  memcpy_field(out_row + req_field->field_offset, field_start,
               blob_length_length);

  // Reserve space for the BLOB data. It is not stored in the output row.
  if (!req_field->reserve_blob_buffer(field_len)) {
    printError("%s: unable to allocate %d bytes for blob", kWho, field_len);
    return -1;
  }

  // Copy the data.
  char* blob_buffer = req_field->get_blob_buffer();
  memcpy(blob_buffer, field_start + blob_length_length, field_len);

  // Write the address of the BLOB data in the row buffer.
  memcpy_field(out_row + req_field->field_offset + blob_length_length,
               (uchar*) &blob_buffer, sizeof(char*));

  set_null_bit(req_field, row_start, table->record[0], out_row);

  // Update number of bytes written and return number of bytes read.
  *bytes_written += blob_length_length + sizeof(char*);
  return (blob_length_length + field_len);
}

int
ha_googlestats::copy_fixed_field(RequestField* req_field,
                                 const uchar* row_start,
                                 int field_offset,
                                 uchar* out_row,
                                 int* bytes_written) {
  const uchar* field_start = row_start + field_offset;
  // Copy the data.
  memcpy_field(out_row + req_field->field_offset, field_start,
               req_field->pack_length);

  set_null_bit(req_field, row_start, table->record[0], out_row);

  // Update number of bytes written and return number of bytes read.
  *bytes_written += req_field->pack_length;
  return req_field->pack_length;
}

// For a variable-length row, copy the currently requested row to the
// output row buffer. The requested row only contains the requested
// columns.
int
ha_googlestats::get_variable_row(uchar * out_row, uchar * requested_row) {
  const char* kWho = "ha_googlestats::get_variable_row";

  // Number of bytes read from input.
  int bytes_read = requested_row_header_length;
  // Number of bytes written to output.
  int bytes_written = 0;

  // Set header to all 0's (by default, nothing is null).
  header_init(out_row, row_header_length);

  RequestField* end = request_fields + request_field_count;
  for (RequestField* req_field = request_fields; req_field != end; ++req_field) {
    int field_read = 0;
    switch (req_field->get_field_type()) {
      case MYSQL_TYPE_VARCHAR:
        field_read = copy_varstring_field(req_field, requested_row, bytes_read,
                                          out_row, &bytes_written);
        if (field_read < 0) {
          printError("%s: cannot read varchar field", kWho);
          return -1;
        }
        break;
      case MYSQL_TYPE_BLOB:
        field_read = copy_blob_field(req_field, requested_row, bytes_read,
                                     out_row, &bytes_written);
        if (field_read < 0) {
          printError("%s: cannot read blob field", kWho);
          return -1;
        }
        break;
      default:
        field_read += copy_fixed_field(req_field, requested_row, bytes_read,
                                       out_row, &bytes_written);
    }
    bytes_read += field_read;
  }

  // Check for out-of-bounds reads.
  if ((requested_row + bytes_read) > (requested_row_buffer + row_bytes)) {
    printError("%s: Input data read beyond end of buffer, used(%d) max(%d)",
               kWho, bytes_read, row_bytes);
    return -1;
  }
  // Check for out-of-bounds writes.
  if (bytes_written > (int)stats.mean_rec_length) {
    printError("%s: Output row written beyond end of buffer, write(%d) max(%lu)",
               kWho, bytes_written, stats.mean_rec_length);
    return -1;
  }
  // Return the number of bytes read from the input buffer.
  return bytes_read;
}

// For a fixed-length row, copy the currently requested row to the
// output row buffer. The requested row only contains the requested
// columns.
int
ha_googlestats::get_fixed_row(uchar* out_row, uchar* requested_row)
{
  const char* kWho = "ha_googlestats::get_fixed_row";

  uchar* requested_row_offset = requested_row_header_length + requested_row;
  int bytes_used = 0; // The number of bytes read and written

  // Set header to all 0's (by default, nothing is null)
  header_init(out_row, row_header_length);

  if (!nullable_request_field_count) {
    // Avoid the 'if' when there are no nullable fields.
    RequestField* end = request_fields + request_field_count;
    RequestField* req_field;
    for (req_field = request_fields; req_field != end; ++req_field) {
      memcpy_field(out_row + req_field->field_offset,
                   requested_row_offset,
                   req_field->pack_length);
      bytes_used += req_field->pack_length;
      requested_row_offset += req_field->pack_length;
    }
  } else {
    RequestField* end = request_fields + request_field_count;
    RequestField* req_field;
    for (req_field = request_fields; req_field != end; ++req_field) {
      memcpy_field(out_row + req_field->field_offset,
                   requested_row_offset,
                   req_field->pack_length);
      bytes_used += req_field->pack_length;
      requested_row_offset += req_field->pack_length;
      set_null_bit(req_field, requested_row, table->record[0], out_row);
    }
  }

  // Check for out-of-bounds reads.
  if ((requested_row + bytes_used) > (requested_row_buffer + row_bytes)) {
    printError("%s: Input data read beyond end of data", kWho);
    return -1;
  }

  // Check for out-of-bounds writes.
  if (bytes_used > (int)stats.mean_rec_length) {
    printError("%s: Output row written beyond end of buffer, write(%d) max(%lu)",
               kWho, bytes_used, stats.mean_rec_length);
    return -1;
  }

  // Return the number of bytes read from the input row.
  return requested_row_length;
}


// TODO: We have almost the exact same code copied into gss_aux.cc; there's
// got to be a better way than this (but I don't see how I can add to a
// va_list).
//
// Print to MySQL error log, just like printError; prefix with query_id.
// This code is adapted from sql/log.cc.
void
ha_googlestats::printError(const char* format, ...)
{
  va_list args;
  time_t skr;
  struct tm tm_tmp;
  struct tm *start;
  va_start(args,format);
  DBUG_ENTER("ha_googlestats::printError");

#ifndef EMBEDDED_LIBRARY
  mysql_mutex_lock(&LOCK_error_log);
#endif

  skr= time(NULL);
  localtime_r(&skr, &tm_tmp);
  start= &tm_tmp;
  fprintf(stderr, "%02d%02d%02d %2d:%02d:%02d %llu ",
          start->tm_year % 100,
          start->tm_mon+1,
          start->tm_mday,
          start->tm_hour,
          start->tm_min,
          start->tm_sec,
          (unsigned long long) current_thd->query_id);
  (void) vfprintf(stderr, format,args);
  (void) fputc('\n', stderr);

  const char* my_table= table->s->table_name.str ? table->s->table_name.str : "<none>";
  const char* my_server= server_name ? server_name : "<none>";

  fprintf(stderr, "  server: %s, port: %d, table: %s\n",
          my_server, server_port, my_table);

  if (!last_request.empty()) {
    // Avoid \n here because the request has one already.
    fprintf(stderr, "  last request: %s", last_request.c_str());
  }

  if (!last_fetch_request.empty() && last_request.find("FETCH_MORE", 0) == 0) {
    // Also print last fetch, if the current request is a fetch_more.
    fprintf(stderr, "  last fetch request: %s", last_fetch_request.c_str());
  }
  fflush(stderr);
  va_end(args);

#ifndef EMBEDDED_LIBRARY
  mysql_mutex_unlock(&LOCK_error_log);
#endif

  DBUG_VOID_RETURN;
}


bool
ha_googlestats::get_error_message(int error, String *buf) {
  DBUG_ASSERT(error >= GSS_ERR_FIRST);

  static const char* default_msg = "Unknown GoogleStats error.";

  unsigned int index = error - GSS_ERR_FIRST;
  if (index >= (sizeof(gss_err_msgs) / sizeof(const char *))) {
    buf->copy(default_msg, strlen(default_msg), system_charset_info);
    return false;
  }
  buf->copy(gss_err_msgs[index], strlen(gss_err_msgs[index]),
            system_charset_info);
  return false;
}


int
ha_googlestats::get_restart_key_parts(StatsServerKeyVals* key_vals,
                                      TABLE* table,
                                      int active_index) {
  const char* kWho = "ha_googlestats::get_restart_key_parts";

  if (table->s->primary_key >= table->s->keys) {
    // It doesn't look like this table has a primary key.
    printError("%s: table does not have a primary key", kWho);
    DBUG_ASSERT(0);
    return -1;
  }
  if (!restart_row || !restart_bytes_used) {
    printError("%s: No restart_row", kWho);
    DBUG_ASSERT(0);
    return -1;
  }

  // Start with everything from the scanned index.
  std::vector<KEY_PART_INFO*> key_parts;
  KEY* key_info = &table->key_info[active_index];
  for (uint i = 0; i < key_info->user_defined_key_parts; ++i)
    key_parts.push_back(&key_info->key_part[i]);

  // Add all key parts from the primary index that are not in the scanned index.
  if (active_index != (int) table->s->primary_key) {
    KEY* pkey_info = &table->key_info[table->s->primary_key];
    for (uint i = 0; i < pkey_info->user_defined_key_parts; ++i) {
      bool found = false;
      for (uint j = 0; j < key_info->user_defined_key_parts; ++j) {
        if (key_info->key_part[j].fieldnr == pkey_info->key_part[i].fieldnr) {
          found = true;
          break;
        }
      }
      if (!found)
        key_parts.push_back(&pkey_info->key_part[i]);
    }
  }
  if (key_parts.empty()) {
    printError("%s: no key parts", kWho);
    DBUG_ASSERT(0);
    return -1;
  }
  if (key_parts.size() > table->s->fields) {
    printError("%s: %d key fields found but table has %d fields",
               kWho, (int)key_parts.size(), table->s->fields);
    DBUG_ASSERT(0);
    return -1;
  }

  // This allocation is local to this call because restart is infrequent.
  uchar* tmp_row = new uchar[stats.mean_rec_length];
  if (!tmp_row) {
    printError("%s: Cannot allocate %lu bytes", kWho, stats.mean_rec_length);
    return GSS_ERR_MEMORY;
  }
  int bytes_read = 0;
  int bytes_written = 0;
  for (uint i = 0; i < key_parts.size(); ++i) {
    Field* field = key_parts[i]->field;
    // Nullable index columns are not supported.
    if (field->real_maybe_null()) {
      printError("%s: Restart key field %d must not be nullable", kWho, i);
      delete [] tmp_row;
      DBUG_ASSERT(0);
      return -1;
    }

    RequestField req_field;
    req_field.pack_length = field->pack_length();
    req_field.field_offset = field->ptr - table->record[0];
    req_field.real_maybe_null = field->real_maybe_null();
    req_field.field = field;
    req_field.set_field_type(field->type());

    // Copy the field to the output row.
    int field_read = 0;
    switch (field->type()) {
      case MYSQL_TYPE_VARCHAR:
        field_read = copy_varstring_field(&req_field, restart_row, bytes_read,
                                          tmp_row, &bytes_written);
        if (field_read < 0) {
          printError("%s: cannot read varchar field", kWho);
          delete [] tmp_row;
          DBUG_ASSERT(0);
          return -1;
        }
        break;
      default:
        field_read = copy_fixed_field(&req_field, restart_row, bytes_read,
                                      tmp_row, &bytes_written);
        break;
    }
    bytes_read += field_read;

    std::string val;
    StatsServerAux::printColVal(tmp_row, key_parts[i], &val);
    key_vals->vals.push_back(val);
  }
  delete [] tmp_row;

  if (bytes_read != restart_bytes_used) {
    printError("%s: Have %d restart bytes but read %d",
               kWho, restart_bytes_used, bytes_read);
    DBUG_ASSERT(0);
    return -1;
  }
  if (bytes_written > (int)stats.mean_rec_length) {
    printError("%s: Wrote %d bytes to restart beyond limit of %lu",
               kWho, bytes_written, stats.mean_rec_length);
    DBUG_ASSERT(0);
    return -1;
  }

  return 0;
}

// Copy the bytes for the restart row to a buffer.  This allows us to restart
// a request when the connection to the current server fails. This is called
// to save the restart key for the packet that has just been consumed when it
// is time to get another packet.
//
// The format for the restart key is (len)(field)*:
//   * len is the number of bytes that follow, stored as a native 4 byte int
//   * field is the field in MySQL format (may be variable length).
int
ha_googlestats::get_restart_row() {
  const char* kWho = "ha_googlestats::get_restart_row";
  if (!restart_bytes_used) {
    // The stats server did not include a restart key.
    return 0;
  }
  // Reallocate the restart key buffer if needed.
  if (restart_row_size < restart_bytes_used) {
    delete [] restart_row;
    restart_row = new uchar[restart_bytes_used];
    if (!restart_row) {
      printError("%s: unable to allocate %d bytes for the restart row",
                 kWho, restart_bytes_used);
      return GSS_ERR_MEMORY;
    }
    restart_row_size = restart_bytes_used;
  }
  // Save the restart key if it exists. Skip the length bytes at the start
  // of the restart key.
  memcpy(restart_row,
         requested_row_buffer + row_bytes + 4,
         restart_bytes_used);
  return 0;
}

// Returns a string for the operator; reverse is used to flip the operator,
// which is useful when the field name is on the RHS.
static std::string get_op_string(Item_func::Functype functype, bool reverse) {
  switch (functype) {
    case Item_func::EQ_FUNC:
      return "=";
    case Item_func::NE_FUNC:
      return "!=";
    case Item_func::LT_FUNC:
      if (!reverse) return "<<";
      return ">>";
    case Item_func::LE_FUNC:
      if (!reverse) return "<=";
      return ">=";
    case Item_func::GE_FUNC:
      if (!reverse) return ">=";
      return "<=";
    case Item_func::GT_FUNC:
      if (!reverse) return ">>";
      return "<<";
    default:
      return "";
  }
  return "";
}

// Returns true if the field is supported for condition pushdowns.  There
// are some restrictions on the type and the field name.
static bool is_valid_field(Field* field) {
  switch (field->real_type()) {
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_ENUM:
      break;
    default:
      return false;
  }
  // Field names should only have alphanumeric characters and '_'.
  int length = strlen(field->field_name);
  for (int i = 0; i < length; ++i) {
    char c = field->field_name[i];
    if (!isalnum(c) && c != '_') return false;
  }
  return true;
}

// Returns a string of a single condition, in the format 'FIELD OP ITEM'.  If
// use_field_name is false, then the format is 'OP ITEM'.  The empty string
// is returned if the condition string cannot be formed.
static std::string get_cond_string(Field* field, const std::string& op,
                                   Item* other_item, bool use_field_name) {
  std::string sub_cond = "";
  if (use_field_name) sub_cond.append(field->field_name);
  sub_cond.append(op);
  if (other_item->const_item() && !other_item->is_null()) {
    // Comparing against a constant.

    // Save the old write_set to restore it later.
    my_bitmap_map *old_map= tmp_use_all_columns(field->table,
                                                field->table->write_set);
    other_item->save_in_field(field, 1);
    // Restore the previous state of the write_set.
    tmp_restore_column_map(field->table->write_set, old_map);

    std::string value;
    StatsServerAux::printColVal(field, &value);
    // ENUM values are empty if they are not valid.
    if (value.empty()) {
      sub_cond.clear();
    } else {
      sub_cond.append(value);
    }
  } else {
    return "";
  }
  return sub_cond;
}

// Checks to see if cond is valid, and returns a string representing the
// conditions to pushdown.  The empty string is returned if the condition
// string cannot be formed.
static std::string validate_cond_pushdown(const Item* cond) {
  std::string condition = "";
  if (cond->type() != Item::FUNC_ITEM) return "";

  Item_func* func_item = (Item_func*)cond;
  switch (func_item->functype()) {
    case Item_func::EQ_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GE_FUNC:
    case Item_func::GT_FUNC: {
      // Simple, binary comparison operators.
      if (func_item->arg_count != 2) {
        // Binary comparison doesn't have 2 arguments.
        return "";
      }
      Item_field* field_item;
      Item* other_item;
      bool reverse = false;
      if (func_item->arguments()[0]->real_item()->type()
          == Item::FIELD_ITEM) {
        // First item is a Field type.
        field_item = (Item_field*)func_item->arguments()[0]->real_item();
        other_item = func_item->arguments()[1]->real_item();
      } else if (func_item->arguments()[1]->real_item()->type()
          == Item::FIELD_ITEM) {
        // Second item is a Field type.
        reverse = true;
        field_item = (Item_field*)func_item->arguments()[1]->real_item();
        other_item = func_item->arguments()[0]->real_item();
      } else {
        // Not comparing a field.
        return "";
      }
      Field* field = field_item->field;

      if (!is_valid_field(field)) return "";

      std::string op = get_op_string(func_item->functype(), reverse);
      if (op == "") return "";
      std::string sub_cond = get_cond_string(field, op, other_item, true);
      if (sub_cond.empty()) return "";
      StatsServerAux::EscapeString(sub_cond.c_str(), sub_cond.length(),
                                   &condition);
      break;
    }
    case Item_func::BETWEEN: {
      Item_func_between *between_item = (Item_func_between*)func_item;
      if (between_item->arg_count != 3) {
        return "";
      }
      Item_field* field_item;
      if (between_item->arguments()[0]->real_item()->type()
          == Item::FIELD_ITEM) {
        field_item = (Item_field*)between_item->arguments()[0]->real_item();
      } else {
        return "";
      }
      Field* field = field_item->field;
      if (!is_valid_field(field)) return "";

      // NOT BETWEEN is not supported, because it would be converted to an OR.
      if (between_item->negated) return "";

      // A BETWEEN B AND C is converted to: A >= B AND A <= C.
      int appended_conds = 0;
      for (int i = 1; i < (int)between_item->arg_count; ++i) {
        std::string op = "";
        if (i == 1) {
          // The min value.
          op.append(">=");
        } else {
          // The max value.
          op.append("<=");
        }
        Item* other_item = between_item->arguments()[i]->real_item();
        std::string sub_cond = get_cond_string(field, op, other_item, true);

        // Since a BETWEEN is an AND of 2 clauses, when one of the conditions
        // fails, the other one can still be passed down.  So, when a
        // 'sub_cond' is empty (failed condition), the empty string does not
        // have to be returned.
        if (!sub_cond.empty()) {
          if (appended_conds > 0) condition.append(",");
          StatsServerAux::EscapeString(sub_cond.c_str(), sub_cond.length(),
                                       &condition);
          appended_conds++;
        }
      }
      break;
    }
    case Item_func::IN_FUNC: {
      Item_func_in *in_item = (Item_func_in*)func_item;
      if (in_item->arg_count < 2) {
        return "";
      }
      Item_field* field_item;
      if (in_item->arguments()[0]->real_item()->type()
          == Item::FIELD_ITEM) {
        field_item = (Item_field*)in_item->arguments()[0]->real_item();
      } else {
        return "";
      }
      Field* field = field_item->field;
      if (!is_valid_field(field)) return "";

      // Check the size of the IN list.
      if (googlestats_pushdown_max_in_size &&
          in_item->arg_count - 1 > googlestats_pushdown_max_in_size) return "";

      std::string in_cond;
      in_cond.append(field->field_name);
      if (in_item->negated) {
        in_cond.append("*!=");
      } else {
        in_cond.append("*=");
      }

      int appended_conds = 0;
      for (int i = 1; i < (int)in_item->arg_count; ++i) {
        Item* other_item = in_item->arguments()[i]->real_item();
        std::string sub_cond = get_cond_string(field, "", other_item, false);
        if (!sub_cond.empty()) {
          if (appended_conds > 0) in_cond.append("|");
          StatsServerAux::EscapeInCondString(sub_cond.c_str(),
                                             sub_cond.length(), &in_cond);
          appended_conds++;
        } else if (!in_item->negated) {
          // Tne entire IN is invalid if one of the elements failed.
          return "";
        }
      }
      if (appended_conds > 0) {
        StatsServerAux::EscapeString(in_cond.c_str(), in_cond.length(),
                                     &condition);
      }
      break;
    }
    default:
      return "";
  }

  return condition;
}

const COND* ha_googlestats::cond_push(const COND *cond) {
  DBUG_ENTER("ha_googlestats::cond_push");

  if (pushed_conds == 0) {
    pushed_conds = new StatsServerPushedConds();
    if (pushed_conds == NULL) {
      printError("ha_googlestats::cond_push: insufficient memory");
      DBUG_RETURN(cond);
    }
  }

  // Conditions to pushdown, separated by commas.
  std::string conditions = "";
  if (cond->type() == Item::COND_ITEM) {
    const Item_cond* cond_item = (const Item_cond*)cond;
    if (cond_item->functype() == Item_func::COND_AND_FUNC) {
      // A list of AND items.
      Item_cond_and* cond_and_item = (Item_cond_and*)cond_item;

      List<Item> *args = cond_and_item->argument_list();
      List_iterator<Item> li(*args);
      Item *item;
      unsigned int appended_conds = 0;
      while ((item = li++)) {
        std::string condition = validate_cond_pushdown(item);
        if (!condition.empty()) {
          if (appended_conds > 0) conditions.append(",");
          conditions.append(condition);
          appended_conds++;
        }
      }
    }
  } else {
    // Just one single item.
    std::string condition = validate_cond_pushdown(cond);
    if (!condition.empty()) conditions.append(condition);
  }

  if (!conditions.empty()) {
    ulong size_bytes = conditions.size();
    // cond_push could be called multiple times with the same conditions.
    bool found_duplicate = false;
    for (int i = 0; i < (int)pushed_conds->conds.size(); ++i) {
      size_bytes += pushed_conds->conds.at(i).size();
      if (pushed_conds->conds.at(i) == conditions) {
        found_duplicate = true;
        break;
      }
    }
    if (!found_duplicate && (googlestats_pushdown_max_bytes == 0 ||
                             size_bytes <= googlestats_pushdown_max_bytes)) {
      pushed_conds->conds.push_back(conditions);
    }
  }
  DBUG_RETURN(cond);
}

void ha_googlestats::cond_pop() {
  if (pushed_conds != 0 && !pushed_conds->conds.empty()) {
    pushed_conds->conds.pop_back();
  }
}

ha_rows ha_googlestats::multi_range_read_info_const(uint keyno,
                                                    RANGE_SEQ_IF *seq,
                                                    void *seq_init_param,
                                                    uint n_ranges,
                                                    uint *bufsz,
                                                    uint *mrr_mode,
                                                    Cost_estimate *cost)
{
  return 1;
}

ha_rows ha_googlestats::multi_range_read_info(uint keyno, uint n_ranges,
                                              uint keys, uint key_parts,
                                              uint *bufsz, uint *mrr_mode,
                                              Cost_estimate *cost)
{
  // Extra buffer size is not needed.
  *bufsz= 0;

  if ((*mrr_mode & HA_MRR_INDEX_ONLY) ||
      (keyno == table->s->primary_key && primary_key_is_clustered()) ||
      key_uses_partial_cols(table->s, keyno))
  {
    // Tell the optimizer we are using the default implementation.  This will
    // disallow BKA for this table, unless allow_default_mrr_bka is on.
    *mrr_mode |= HA_MRR_USE_DEFAULT_IMPL;
  }

  cost->reset(); // Assume random seeks.

  // With respect to RPCs, using MRR is similar to just a single FETCH.
  // Adding 'n_ranges' is the added cost of the stats-server having to seek to
  // each new range.
  cost->io_count= read_time(keyno, 1, keys) + n_ranges;
  return 0;
}

int ha_googlestats::multi_range_read_init(RANGE_SEQ_IF *seq_funcs,
                                          void *seq_init_param,
                                          uint n_ranges, uint mode,
                                          HANDLER_BUFFER *buf)
{
  const char* kWho = "ha_googlestats::multi_range_read_init";
  UpdateProcInfo updater(kWho);

  if (googlestats_log_level >= GsLogLevelHigh) {
    printError("%s: for %s", kWho, table->s->table_name.str);
  }

  if (sock_fd < 0) {
    printError("%s: no server connection", kWho);
    // init() didn't succeed
    return(last_connection_error);
  }

  fetch_multi_enabled = false;

  mrr_iter= seq_funcs->init(seq_init_param, n_ranges, mode);
  mrr_funcs= *seq_funcs;
  mrr_is_output_sorted= MY_TEST(mode & HA_MRR_SORTED);
  mrr_have_range= FALSE;


  // Verify all ranges are EQ.
  int range_res;
  while (!(range_res= mrr_funcs.next(mrr_iter, &mrr_cur_range))) {

    if (!(mrr_cur_range.range_flag & EQ_RANGE)) {
      fetch_multi_enabled = false;
      return handler::multi_range_read_init(seq_funcs, seq_init_param,
                                            n_ranges, mode, buf);
    }
  }

  fetch_multi_enabled = true;

  if (orig_keys != 0) {
    delete orig_keys;
    orig_keys = 0;
  }

  if (n_ranges > 0) {
    orig_keys = new StatsServerKeys();
    if (orig_keys == NULL) {
      printError("ha_googlestats::server_scan_start: insufficient memory");
      return(-1);
    }
  }

  int range_offset = 0;
  mrr_iter= seq_funcs->init(seq_init_param, n_ranges, mode);
  while (!(range_res= mrr_funcs.next(mrr_iter, &mrr_cur_range))) {

    KEY* key_info = &table->key_info[active_index];
    for (uint i = 0, key_offset = 0;
         key_offset < mrr_cur_range.start_key.length;
         ++i) {

      if (i == 0) {
        orig_keys->vals.push_back(std::vector<std::string>());
      }

      KEY_PART_INFO* part_info = &key_info->key_part[i];
      Field* field = part_info->field;
      memcpy(field->ptr, mrr_cur_range.start_key.key + key_offset,
             part_info->store_length);
      if (field->is_null()) {
        orig_keys->vals[range_offset].push_back("NULL");
      } else {
        String tmp;
        get_key_val_str(field, &tmp);
        orig_keys->vals[range_offset].push_back(tmp.c_ptr_safe());
      }
      key_offset += part_info->store_length;

    }

    range_offset++;
  }

  orig_find_flag = HA_READ_KEY_EXACT;
  orig_request_type = FetchMulti;
  scan_received_rows = false; // We haven't seen anything yet.

  // Request 2 keys for the first FETCH.
  num_requested_keys = 2;

  // Start scanning from the first key.
  keys_index = 0;
  int res = 0;
  if ((res = server_scan_start(0))) {
    printError("%s: cannot start server scan", kWho);
    return(res);
  }

  // Set mrr_cur_range to the first range.
  mrr_iter= seq_funcs->init(seq_init_param, n_ranges, mode);
  mrr_range_done = mrr_funcs.next(mrr_iter, &mrr_cur_range);
  return(0);
}

// Returns true if the row pointed to by key_info matches the values in key.
// length is the length of key, in bytes.
static bool match_row_to_range(KEY* key_info, uint length, const uchar* key) {
  for (uint i = 0, key_offset = 0; key_offset < length; ++i) {
    KEY_PART_INFO* part_info = &key_info->key_part[i];
    Field* field = part_info->field;

    if (memcmp(field->ptr, key + key_offset, part_info->store_length)) {
      // The returned row has different values from the current range values.
      return false;
    }
    key_offset += part_info->store_length;
  }
  return true;
}

int ha_googlestats::multi_range_read_next(range_id_t *range_info)
{
  const char* kWho = "ha_googlestats::read_multi_range_next";
  UpdateProcInfo updater(kWho);
  if (googlestats_log_level >= GsLogLevelHigh) {
    printError("%s: for %s", kWho, table->s->table_name.str);
  }

  if (sock_fd < 0) {
    printError("%s: no server connection", kWho);
    // init() didn't succeed
    return(last_connection_error);
  }

  if (!fetch_multi_enabled) {
    return handler::multi_range_read_next(range_info);
  }

  int res = 0;

  do {
    res = get_buffer_row(table->record[0]);

    if (res == HA_ERR_END_OF_FILE) {
      if (keys_index < orig_keys->vals.size()) {
        // There are more keys to request. Start the next scan from keys_index.
        scan_received_rows = false;
        restart_bytes_used = 0;
        int scan_res = 0;

        // Double the number of requested keys.
        num_requested_keys *= 2;

        // Move on to the next multi-range.
        mrr_range_done = mrr_funcs.next(mrr_iter, &mrr_cur_range);

        if ((scan_res = server_scan_start(0))) {
          printError("%s: cannot start server scan", kWho);
          return(scan_res);
        }
        continue;
      }
      fetch_multi_enabled = false;
      table->status = STATUS_NOT_FOUND;
      return(my_errno = HA_ERR_END_OF_FILE);
    }
    if (res != 0) {
      fetch_multi_enabled = false;
      printError("%s: cannot get more rows", kWho);
      return(res);
    }
  } while(res != 0);

  // Find the correct range.
  bool found_correct_range = false;
  KEY* key_info = &table->key_info[active_index];
  while (!found_correct_range && !mrr_range_done) {
    if (!match_row_to_range(key_info, mrr_cur_range.start_key.length,
                            mrr_cur_range.start_key.key)) {
      // This row does not match the current range.
      // Move on to the next multi-range.
      mrr_range_done = mrr_funcs.next(mrr_iter, &mrr_cur_range);
      found_correct_range = false;
    } else {
      found_correct_range = true;
    }
  }

  *range_info = mrr_cur_range.ptr;
  table->status = 0;
  return(0);
}

// GoogleStats plugin initialization.
static handler *googlestats_create_handler(handlerton *hton,
                                           TABLE_SHARE *table,
                                           MEM_ROOT *mem_root)
{
  return new (mem_root) ha_googlestats(hton, table);
}

static int googlestats_init(void *p)
{
  DBUG_ENTER("ha_googlestats::googlestats_init");

  handlerton *googlestats_hton = (handlerton *)p;

  googlestats_hton->state = SHOW_OPTION_YES;
  googlestats_hton->db_type = DB_TYPE_GOOGLESTATS;
  googlestats_hton->create = googlestats_create_handler;

  bool success = true;

  // Initialize the stats server cache.
  success = StatsServerCache::createInstance();

  // init() returns 0 for success, non-zero for failure.
  // This code is written this way in the interest of being explicit.
  DBUG_RETURN(success ? 0 : 1);
}

static int googlestats_deinit(void *p)
{
  DBUG_ENTER("ha_googlestats::googlestats_end");

  // destroyInstance() is safe to call even if it was never created in the
  // first place.
  bool success = StatsServerCache::destroyInstance();

  // deinit() returns 0 for success, non-zero for failure.
  // This code is written this way in the interest of being explicit.
  DBUG_RETURN(success ? 0 : 1);
}


static struct st_mysql_storage_engine googlestats_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

// Plugin system variables.
static MYSQL_SYSVAR_STR(servers_table, googlestats_servers_table,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Name of the table that contains stats servers configuration data",
  NULL, NULL, NULL);

static MYSQL_SYSVAR_STR(version_table, googlestats_version_table,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Name of the table that contains current version number",
  NULL, NULL, NULL);

static MYSQL_SYSVAR_INT(timeout, googlestats_timeout,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Timeout in seconds for server communications",
  NULL, NULL, 10, 0, 600, 0);

static MYSQL_SYSVAR_INT(retry_interval, googlestats_retry_interval,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Time interval in seconds between last time of access and retry of unreachable server",
  NULL, NULL, 0, 0, 600, 0);

static MYSQL_SYSVAR_INT(log_level, googlestats_log_level,
  PLUGIN_VAR_RQCMDARG,
  "Level of logging (0: none, 1: low, 2: medium, 3: high)",
  NULL, NULL, 0, 0, 3, 0);

static MYSQL_SYSVAR_INT(max_packet_size, googlestats_max_packet_size,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Maximum size of a packet accepted from a stats server",
  NULL, NULL, 8388608, 0, 1000000000, 0);

static MYSQL_SYSVAR_ULONG(pushdown_max_bytes, googlestats_pushdown_max_bytes,
  PLUGIN_VAR_OPCMDARG,
  "Maximum number of condition bytes to pushdown to GoogleStats servers "
  "(0 means unlimited)",
  NULL, NULL, 1000, 0, 1000000, 0);

static MYSQL_SYSVAR_ULONG(pushdown_max_in_size, googlestats_pushdown_max_in_size,
  PLUGIN_VAR_OPCMDARG,
  "Maximum allowable number of elements in an IN list for condition pushdown "
  "(0 means unlimited)",
  NULL, NULL, 10, 0, 100, 0);

static MYSQL_SYSVAR_ULONG(estimate_interval, googlestats_estimate_interval,
  PLUGIN_VAR_OPCMDARG,
  "Time interval (in seconds) between consecutive calls to stats servers",
  NULL, NULL, 43200, 0, 86400, 0);

static MYSQL_SYSVAR_ULONG(remote_tier, googlestats_remote_tier,
  PLUGIN_VAR_OPCMDARG,
  "Tier number of the first stats server tier considered to be remote",
  NULL, NULL, 3, 0, 10, 0);

static MYSQL_SYSVAR_ULONG(write_tries, googlestats_write_tries,
  PLUGIN_VAR_OPCMDARG,
  "Number of tries to write to socket",
  NULL, NULL, 2, 1, 5, 0);

static struct st_mysql_sys_var* googlestats_system_variables[]= {
  // Global variables.
  MYSQL_SYSVAR(log_level),
  MYSQL_SYSVAR(max_packet_size),
  MYSQL_SYSVAR(pushdown_max_bytes),
  MYSQL_SYSVAR(pushdown_max_in_size),
  MYSQL_SYSVAR(retry_interval),
  MYSQL_SYSVAR(servers_table),
  MYSQL_SYSVAR(timeout),
  MYSQL_SYSVAR(version_table),
  MYSQL_SYSVAR(estimate_interval),
  MYSQL_SYSVAR(remote_tier),
  MYSQL_SYSVAR(write_tries),
  // Thread (session) variables.
  MYSQL_SYSVAR(initial_fetch_rows),
  MYSQL_SYSVAR(multifetch_max_keys),
  MYSQL_SYSVAR(query_on_update),
  NULL
};

maria_declare_plugin(googlestats)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &googlestats_storage_engine,
  "GoogleStats",
  "Google",
  "Supports Stats Server tables",
  PLUGIN_LICENSE_PROPRIETARY,
  googlestats_init,   /* Plugin Init */
  googlestats_deinit, /* Plugin Deinit */
  0x0100              /* 1.0 */,
  NULL,               /* status variables */
  googlestats_system_variables,               /* system variables */
  NULL                /* reserved */
}
maria_declare_plugin_end;
