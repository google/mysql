// Copyright 2013 Google Inc. All Rights Reserved.

#ifndef HA_GOOGLESTATS_H
#define HA_GOOGLESTATS_H

#include <string>
#include "mysql_version.h"
#include "field.h"

struct StatsServerKeys;
struct StatsServerKeyVals;
class StatsServerConnectState;
struct StatsServerPushedConds;
class StatsServerConnectState;       // gss_cache.h

// Used for FLUSH STATS_SERVERS. Returns false on success.
extern bool googlestats_reinit(THD*);

extern char *googlestats_servers_table;
extern char *googlestats_version_table;
extern int googlestats_timeout;
extern int googlestats_retry_interval;
extern int googlestats_log_level;
extern my_bool buffer_table_sort;
extern int googlestats_max_packet_size;

extern int get_versions_for_googlestats_tables(THD* thd, TABLE_LIST* tables);

enum GsLogLevel {
  GsLogLevelNone = 0,
  GsLogLevelLow,
  GsLogLevelMed,
  GsLogLevelHigh
};

// This class describes a "requested" field; i.e. one that has been requested
// on the query.  We also cache commonly used field attributes in order to
// speed up row copying.
class RequestField
{
  // Please keep these fields as 'public'.  They are used VERY frequently when
  // copying requested rows.  If you choose to make them private but accessible
  // via accessors, then make sure the accessors are declared as 'inline' to
  // get the best performance!
public:
  int field_offset; // Computed from field.
  int pack_length; // From field->pack_length().
  Field* field;

  char* get_blob_buffer() const { return blob_buffer; }

  enum_field_types get_field_type() const { return field_type; }
  void set_field_type(enum_field_types val) { field_type = val; }

  // For dealing with nullable fields.
  int req_null_offset; // Offset of null byte in requested row (not row buffer).
  int req_null_bit; // Bit mask for null bit in requested row (not row buffer).
  bool real_maybe_null;  // From field->real_maybe_null().

  RequestField() {
    init();
  }
  ~RequestField() {
    release_blob_buffer();
  }
  bool reserve_blob_buffer(int size) {
    if (blob_buffer_size >= size)
      return true;
    blob_buffer = new char[size];
    if (!blob_buffer) {
      return false;
    } else {
      blob_buffer_size = size;
      return true;
    }
  }
  void release_blob_buffer() {
    delete [] blob_buffer;
    blob_buffer = (char*) NULL;
    blob_buffer_size = 0;
  }
  void init() {
    field_offset = 0;
    pack_length = 0;
    field = 0;
    req_null_offset = 0;
    req_null_bit = 0;
    real_maybe_null = false;
    blob_buffer = (char*) NULL;
    blob_buffer_size = 0;
    // Use a value not supported by a stats server.
    field_type = FIELD_TYPE_GEOMETRY;
  }

 private:
  // TODO: Deallocate blob buffers early.
  // Buffer used for lob columns (text, blob) that are stored out of line
  // and not in the row buffer. A pointer is stored in the row buffer. The
  // table handler must manage the memory pointed to.
  char* blob_buffer;
  int blob_buffer_size;

  // Cache the value of field->type()
  enum_field_types field_type;
};

// TODO: Use the table creation comment, update_table_comment(), update_create_info().
// TODO: Support get_error_message().
// TODO: Support check(), optimize(), and analyze().
// TODO: Prevent position() from being called.

class ha_googlestats: public handler
{
  int rowCount;
  int sock_fd;
  int version_num;
  longlong version_hash;
  int32 schema_checksum;

  THR_LOCK thread_lock;
  THR_LOCK_DATA lock;

  // Cached selectivity estimates.
  ha_rows rows_in_tbl; // Total number of rows in table.
  time_t sel_est_last_time; // Time value of the last estimate.

  // If true, we haven't read all rows from the last scan request.
  bool rows_left_to_fetch;

  // Number of FETCH requests issued for current query.
  int num_fetch_requests;

  // Needed in case we need to mark server as dead.
  char* server_name;
  int server_port;
  unsigned int server_tier;

  uchar* requested_row_buffer;  // Uncompressed packet with rows and restart key.
  int requested_row_buffer_size;  // Number of bytes allocated to requested_row_buffer.

  uchar* data_buffer; // Compressed packet with rows and the restart key
  int data_buffer_size; // #bytes allocated to data buffer

  // The restart key may be added to the end of a fetch response packet.
  // It is the value used to restart the fetch should the connection to
  // the current stats server get broken. It includes values in the MySQL
  // format for all columns required to position a cursor.
  uchar* restart_row;      // Buffer for row used to restart scan.
  int restart_row_size;   // The size of restart_row.
  int restart_bytes_used; // Number of bytes in restart_row used for the restart key.

  int num_buffered_rows; // Number of rows currently in buffer.
  int max_buffered_rows; // Maximum number of rows in buffer.
  int num_requested_rows; // Number of rows requested in last scan.
  int num_received_rows; // Number of rows received so far since last request.

  // Number of bytes used for rows in the uncompressed packet, set per packet.
  int32 row_bytes;

  bool scan_received_rows; // Set to true if at least one row has been received.

  int requested_row_header_length; // Length of header of row coming from server.
  int requested_row_length; // Length of row with only requested columns.
  int nullable_request_field_count; // Number of nullable columns requested by last query.

  std::string last_request; // Most recently sent request string.
  std::string last_fetch_request; // Most recently sent FETCH request string.

  ulong fetch_calls;    // Number of FETCH and FETCH_MORE commands.
  ulong nonfetch_calls; // Number of commands other than FETCH and FETCH_MORE.

  // Time waiting on read() after ...
  ulonglong fetch_mics;     // ... FETCH and FETCH_MORE commands.
  ulonglong nonfetch_mics;  // ... commands other than FETCH and FETCH_MORE.

  ulonglong fetch_rows; // Number of rows fetched.
  ulonglong fetch_bytes; // Number of bytes fetched.

  // Holds all the keys of the original FETCH command.
  StatsServerKeys* orig_keys;
  enum ha_rkey_function orig_find_flag;
  enum RequestType {Fetch, FetchLast, FetchMulti};
  RequestType orig_request_type;

  // The conditions pushed down from MySQL.
  StatsServerPushedConds* pushed_conds;

  // TODO(jeremycole): Seems like this is probably unnecessary?
  // TODO(gpang): For old MRR interface:
  // The range count for multi-range reads.
  //  ulong range_count;

  // True if there are no more MRR ranges to scan.
  int mrr_range_done;

  // The index into the vector orig_keys, to start the FETCH from.
  uint keys_index;
  // The index into the vector orig_keys, of the last FETCH command, used for
  // restarting a FETCH.
  int keys_index_for_restart;
  // # of keys requested in last fetch, used for FETCH_MULTI.
  uint num_requested_keys;

  // True if FETCH_MULTI can be used to fetch the ranges.  If all the ranges
  // are equalities, then FETCH_MULTI is possible.
  // FETCH_MULTI is a new feature of the stats-server which will allow
  // the storage engine to specify multiple keys in a single FETCH command.
  // The protocol was extended to allow specifying multiple keys.  For example:
  // FETCH ... Keys={key=1,2,A,B } Keys={key=3,4,A,B } ...
  bool fetch_multi_enabled;

  // True if the restart key should just be used as a binary restart key.
  // If the stats-server sends back a binary restart key, the googlestats
  // storage engine will NOT parse and interpret the restart key, but simply
  // send back the same data for the stats-server to use.
  bool binary_restart_key;

  // Get a connection to a stats server. 'state' is used to avoid connecting
  // to the same server more than twice on subsequent calls to connect_server.
  // indexName is the name of the index to be used. Return 0 on success.
  int connect_server_with_state(StatsServerConnectState* state,
                                const char* indexName);

  // Convenience method to call connect_server_with_state without passing
  // 'state' argument. indexName is the name of the index to be used. Return
  // 0 on success.
  int connect_server(const char* indexName);

  // Return the name of the active index or null.
  char* active_index_name();

  int server_scan_start(const uchar* key, uint key_len,
                        enum ha_rkey_function find_flag, RequestType reqType);
  int server_scan_start(StatsServerKeyVals* start_key_vals);
  int server_scan_init();
  int server_scan_restart();
  int server_scan_next();
  int fill_row_buffer();
  int _fill_row_buffer();

  // The following functions copy a field for a row fetched from a stats
  // server into an output row. The output row uses the format expected
  // by MySQL. Fields in the output row are fixed length. varchar fields
  // are padded to their max length. blob fields stored a fixed length pointer
  // in the output row. The blob data is stored elsewhere and the table handler
  // is responsible for managing that memory. Return the number of bytes read
  // from the input on success and -1 on failure. The functions are specialized
  // by input field datatype.

  // Copy a varchar field. Return number of bytes read.
  // Parameters:
  //   req_field - describes the field to copy
  //   row_start - start of the input row
  //   field_offset - offset of the field in the input row
  //   out_row - start of the output row
  //   bytes_written - #bytes written to out_row
  int copy_varstring_field(RequestField* req_field,
                           const uchar* row_start,
                           int field_offset,
                           uchar* out_row,
                           int* bytes_written);

  // Copy a blob field (TEXT or BLOB). Return number of bytes read.
  int copy_blob_field(RequestField* req_field,
                      const uchar* row_start,
                      int field_offset,
                      uchar* out_row,
                      int* bytes_written);

  // Copy a fixed length field. Return number of bytes read.
  inline int copy_fixed_field(RequestField* req_field,
                              const uchar* row_start,
                              int field_offset,
                              uchar* out_row,
                              int* bytes_written);

  // Read header for the response to a FETCH requestion in version 3 of
  // the stats server protocol. Return 0 on success.
  // Parameters:
  //   packed_length - returns the value of 'packed_length'
  //   unpacked_length - returns the value of 'unpacked_length'
  //   num_rows - returns the value of 'num_rows'
  //   bytes_for_rows - returns the value of 'bytes_for_rows'
  //
  // The format of a version 3 response header is:
  //   signature       - 4 bytes with the values 'SS30'
  //   packed_length   - 4 bytes, length of the compressed data that
  //                     follows the header
  //   unpacked_length - 4 bytes, length of the data when uncompressed
  //   num_rows        - 4 bytes, number of rows in the response packet
  //   bytes_for_rows  - 4 bytes, number of bytes in uncompressed packet
  //                     used for row data. Remaining bytes are used for
  //                     the fetch restart key.
  int read_version_3_header(int32* packed_length,
                            int32* unpacked_length,
                            int32* num_rows,
                            int32* bytes_for_rows);

  // Finish reading version 3 protocol packet for the response to a FETCH
  // request. ::read_version_3_header() must have been called prior to this.
  // Return 0 on success.
  // Parameters:
  //   packed_length - length of the data when compressed
  //   unpacked_length - length of the data when uncompressed
  //   num_rows - number of rows in the packet
  int read_version_3_data(int packed_length,
                          int unpacked_length,
                          int num_rows);

  int get_buffer_row(uchar* buf);
  int get_version_num();
  int request_columns(std::string* columns_request);

  // Determine whether all fields have fixed length. Set fixed_length_row
  // to true when all fields are fixed length. Returns 0 on success.
  int classify_fields();

  // Copy the requested columns into the output buffer. Return the number
  // of bytes consumed from requested_row_buffer and -1 on failure.
  // Set to get_fixed_row or get_version_3_row.
  typedef int (ha_googlestats::*get_row_fn)(uchar* to, uchar* requested_row);
  get_row_fn get_requested_row;

  // Extract a fixed length row.
  int get_fixed_row(uchar* to, uchar* requested_row);

  // Extract a possibly variable length row.
  int get_variable_row(uchar* to, uchar* requested_row);

  // Copy the restart key bytes to restart_row. Return 0 on success.
  int get_restart_row();

  // Get the values for the restart key fields from restart_row.
  // Return -1 on failure.
  int get_restart_key_parts(StatsServerKeyVals* key_vals,
                            TABLE* table,
                            int active_index);

  // Get selectivity estimates for the index 'keyinfo' and return the number
  // of rows in the table. Retries subsequent tiers if failure occurs.
  longlong get_sel_estimate(StatsServerConnectState *state, KEY* keyinfo);

  // Get selectivity estimates for all indexes and return 0 on success.
  int get_sel_estimates();
  int32 compute_schema_checksum(bool* ok);

  // Prints error messages to the database error log.
  void printError(const char* format, ...)
      __attribute__((__format__(__printf__, 2, 3)));

  // Close sock_fd if open. Copy timing stats to global counters.
  void cleanup();

  // List of requested fields and related info..
  int last_query_id;   // the latest query id
  int request_field_count;  // request_fields current array count

  int request_field_max; // request_fields max array count (same as
                         // table->fields; used as a sanity check)
  int row_header_length;  // header length for row_buffer
  RequestField* request_fields; // list of fields selected in the request
  uchar* next_requested_row; // pointer to the next requested row

  // True when all of the fetched fields are fixed length.
  bool fixed_length_row;

  // The error code of the last failed connection.
  int last_connection_error;

  // Fudge factor for optimizer costs (count an RPC as 10 file reads)
  static const int kIoNoCache = 10;

public:

  ha_googlestats(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_googlestats();

  // Release resources held by this instance (memory, network connections).
  void release_resources();

  virtual bool primary_key_is_clustered() { return true; }

  virtual bool get_error_message(int error, String *buf);

  // Cost in IOs to scan a GoogleStats table.
  virtual double scan_time();

  // Cost in IOs to probe a GoogleStats index 'ranges' time to fetch
  // 'rows' rows (fetch rows/ranges rows per probe).
  virtual double read_time(uint index, uint ranges, ha_rows rows);

  virtual const char *index_type(uint key_number) { return "BTREE";}

  virtual int open(const char *name, int mode, uint test_if_locked);
  virtual int close(void);

  // Methods for keyed access (by index).
  virtual int index_init(uint idx, bool sorted);
  virtual int index_end();
  virtual int index_read(uchar * buf, const uchar * key,
			 uint key_len, enum ha_rkey_function find_flag);
  virtual int index_read_idx(uchar * buf, uint index, const uchar * key,
			     uint key_len, enum ha_rkey_function find_flag);
  virtual int index_next(uchar * buf);
  virtual int index_first(uchar * buf);
  virtual int index_last(uchar * buf);
  virtual int index_next_same(uchar *buf, const uchar *key, uint keylen);

  // Methods for full scan.
  virtual int rnd_init(bool scan);
  virtual int rnd_end();
  virtual int rnd_next(uchar * buf);
  virtual int rnd_pos(uchar * buf, uchar * pos) { return my_errno=HA_ERR_WRONG_COMMAND; }

  virtual ha_rows records_in_range(uint inx, key_range* min_key, key_range* max_key);

  virtual void position(const uchar *record);
  virtual int info(uint);
  virtual int extra(enum ha_extra_function operation);
  virtual int reset();
  virtual int external_lock(THD *thd, int lock_type);

  // The following can be called without an open handler.
  virtual const char *table_type() const { return "GoogleStats"; }
  virtual const char **bas_ext() const;
  virtual ulonglong table_flags(void) const
  {
    return(HA_REC_NOT_IN_SEQ |
           HA_CAN_INDEX_BLOBS |
           HA_CAN_SQL_HANDLER |
           HA_PRIMARY_KEY_IN_READ_INDEX |
           HA_TABLE_SCAN_ON_INDEX |
           HA_NO_AUTO_INCREMENT |
           HA_NO_RND_POS);
  }
  virtual ulong index_flags(uint idx, uint part, bool all_parts) const
  {
    // HA_NO_READ_PREFIX_LAST is used to disable calls to index_read_last()
    return (HA_READ_NEXT |
            HA_READ_ORDER |
            HA_READ_RANGE |
            HA_KEYREAD_ONLY |
            HA_NO_READ_PREFIX_LAST);
  }

  // This is very important. Without it full table scans + sort will be used in
  // place of index scans.
  const key_map *keys_to_use_for_scanning() { return &key_map_full; }

  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table,
      Alter_inplace_info *ha_alter_info)
  {
    return HA_ALTER_INPLACE_NO_LOCK;
  }

  bool prepare_inplace_alter_table(TABLE *altered_table,
                                         Alter_inplace_info *ha_alter_info)
  {
    return false;
  }

  bool inplace_alter_table(TABLE *altered_table,
                           Alter_inplace_info *ha_alter_info)
  {
    return false;
  }

  bool commit_inplace_alter_table(TABLE *altered_table,
                                  Alter_inplace_info *ha_alter_info,
                                  bool commit)
  {
    return false;
  }

  virtual uint max_supported_keys() const { return 1000; }
  virtual uint max_supported_key_parts() const { return MAX_REF_PARTS; }
  virtual uint max_supported_key_part_length() const { return MAX_KEY_LENGTH; }

  // TODO: Store the create comment and pass it to the stats server.
  virtual int create(const char *name, TABLE *form, HA_CREATE_INFO *info);

  // Override the default implementation because there are no files on the server.
  virtual int delete_table(const char *name) { return 0; }
  virtual int rename_table(const char* from, const char* to) { return 0; }

  virtual THR_LOCK_DATA **store_lock(THD *thd,
				     THR_LOCK_DATA **to,
				     enum thr_lock_type lock_type);

  // This returns HA_POS_ERROR because records is not always exact and were
  // a value returned that was too small (see handler.h for the default)
  // filesort may dump core. When this is returned, filesort uses the max
  // sized sort buffer. None of this should matter as all stats tables should
  // be large enough that the max size sort buffer whould have been used
  // anyway.
  virtual ha_rows estimate_rows_upper_bound() { return HA_POS_ERROR; }

  virtual const COND *cond_push(const COND *cond);
  virtual void cond_pop();

  virtual ha_rows multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                              void *seq_init_param,
                                              uint n_ranges, uint *bufsz,
                                              uint *mrr_mode,
                                              Cost_estimate *cost);
  virtual ha_rows multi_range_read_info(uint keyno, uint n_ranges, uint keys,
                                        uint key_parts, uint *bufsz,
                                        uint *mrr_mode, Cost_estimate *cost);
  virtual int multi_range_read_init(RANGE_SEQ_IF *seq_funcs,
                                    void *seq_init_param,
                                    uint n_ranges, uint mode,
                                    HANDLER_BUFFER *buf);
  virtual int multi_range_read_next(range_id_t *range_info);

  friend class IncrementNonfetch;
  friend class IncrementFetch;
};

#endif // HA_GOOGLESTATS_H
