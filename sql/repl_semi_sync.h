/*
  Copyright 2007 Google Inc. All Rights Reserved.
  Author: wei@google.com (Wei Li)

  Repl_semi_sync class is responsible for semi synchronous replication. The
  general idea of semi-sync replication is the master database needs to make
  sure that the slave database receives its replication events before telling
  the client that a transaction has been committed. The difference between
  semi-sync and full-sync is that full-sync replication requires that the
  slave database finish replicated transaction before replying the master.

  The current semi-sync implementation defines a transaction wait timeout.
  In this way, the master would not wait for the slave indefinitely;
  instead after a configurable timeout, the master will continue the current
  transaction. At the same time, semi-sync will be disabled so that no
  transactions will wait after this. Later, semi-sync can be enabled again
  when the slave catches up in replication. The timeout design is to prevent
  the master from halting for update, in case of the slave machine issues or
  network issues.
*/

#ifndef REPL_SEMI_SYNC_INCLUDED
#define REPL_SEMI_SYNC_INCLUDED

/* So that OPTIONS_WRITTEN_TO_BIN_LOG in log_event.h uses the right values. */
#include "mysql_priv.h"
#include "log_event.h"                        /* for Log_event_type */

class THD;
class String;
typedef struct st_net NET;
class Repl_semi_sync;

/** The Repl_semi_sync instance. */
extern Repl_semi_sync semi_sync_replicator;

/* mysqld command-line options. See mysqld.cc for documentation. */
extern ulong rpl_event_buffer_size;
extern ulong rpl_semi_sync_enabled;
extern my_bool rpl_semi_sync_always_on;
extern ulong rpl_semi_sync_slave_enabled;
extern ulong rpl_semi_sync_timeout;
extern ulong rpl_semi_sync_trace_level;

/* We export several status variables to track the semi-sync on the master. */
extern ulong rpl_semi_sync_status;
extern ulong rpl_semi_sync_slave_status;
extern ulong rpl_semi_sync_yes_transactions;
extern ulong rpl_semi_sync_no_transactions;
extern ulong rpl_semi_sync_off_times;
extern ulong rpl_semi_sync_timefunc_fails;
extern ulong rpl_semi_sync_num_timeouts;
extern ulong rpl_semi_sync_wait_sessions;
extern ulong rpl_semi_sync_back_wait_pos;
extern ulong rpl_semi_sync_trx_wait_time;
extern ulong rpl_semi_sync_net_wait_time;
extern ulonglong rpl_semi_sync_net_wait_num;
extern ulonglong rpl_semi_sync_trx_wait_num;
extern ulonglong rpl_semi_sync_net_wait_total_time;
extern ulonglong rpl_semi_sync_trx_wait_total_time;

extern ulong rpl_semi_sync_clients;

/**
  This class contains the state and flow methods used for
  semi-sync replication. It has code for both the master and the
  slave side. However, the slave side is relatively lightweight
  because it mostly consists of a stateless reply of binlog
  events sent from the master.

  Master flow:
    - Session/Thread with a commit transaction:
      - Write all events for the current transaction to binlog.
      - Call report_binlog_offset() to report the master's
        position.
      -   Call write_tranx_in_binlog().
      - Call commit_trx() after storage engines' final commit
        action to wait for slave's reply.
    - binlog_dump session/thread - <mysql_binlog_send>
      - Connection from slave to dump binlog events
      - While (true):
      -   Call reserve_sync_header() to create header for the
          next event.
      -   Read the event from binlog.
      -   Call update_sync_header() to indicate whether slave
          should reply.
      -   Call read_slave_reply() to wait for slave's reply.
      -     Call report_reply_binlog() to record the slaves'
            highest position.

  Slave flow:
    - Call slave_read_sync_header() to check if a reply is
      needed.
    - Call slave_reply() to report replication progress to the
      master.

*/
class Repl_semi_sync {
private:

  /**
    This class manages memory for active transaction list. We record each
    active transaction with a Tranx_node. Because each session can
    only have only one open transaction, the total active
    transaction nodes can not exceed the maximum sessions.
    Currently in MySQL, sessions are the same as connections.
  */
  class Active_tranx {
  private:
    struct Tranx_node {
      char              *log_name;
      my_off_t           log_pos;
      /** Next node in the sorted list. */
      struct Tranx_node *next;
      /** Next node during hash collision. */
      struct Tranx_node *hash_next;
    };

    /** Maintains the active transaction list. */
    Tranx_node      *node_array;
    Tranx_node      *free_pool;

    /** Active transaction list in sort order. */
    Tranx_node      *trx_front;
    /** Active transaction list in sort order. */
    Tranx_node      *trx_rear;

    /** A hash table on active transactions. */
    Tranx_node     **trx_htb;

    int              num_transactions;        /**< maximum transactions */
    int              num_entries;             /**< maximum hash table entries */
    pthread_mutex_t *lock;                    /**< mutex lock */
    ulong           *trace_level;             /**< trace level */

    inline void assert_lock_owner();
    inline void function_enter(const char *func_name);
    inline int  function_exit(const char *func_name, int exit_code);

    inline Tranx_node *alloc_tranx_node();

    inline uint calc_hash(const uchar *key,uint length);
    uint get_hash_value(const char *log_file_name, my_off_t log_file_pos);

    int compare(const char *log_file_name1, my_off_t log_file_pos1,
                const Tranx_node *node2)
    {
      return compare(log_file_name1, log_file_pos1,
                     node2->log_name, node2->log_pos);
    }
    int compare(const Tranx_node *node1,
                const char *log_file_name2, my_off_t log_file_pos2)
    {
      return compare(node1->log_name, node1->log_pos,
                     log_file_name2, log_file_pos2);
    }
    int compare(const Tranx_node *node1, const Tranx_node *node2)
    {
      return compare(node1->log_name, node1->log_pos,
                     node2->log_name, node2->log_pos);
    }

  public:
    /**
      Create an Active_tranx instance.

      @param max_connections  Maximum number of concurrent
                              connections expected.
      @param in_lock          Repl_semi_sync's lock variable
      @param in_trace_level   Repl_semi_sync trace level variable
    */
    Active_tranx(int max_connections, pthread_mutex_t *in_lock,
                 ulong *in_trace_level);
    ~Active_tranx();

    /**
      Insert an active transaction node with the specified position.

      @return
        @retval  0  success
        @retval -1  error
    */

    int insert_tranx_node(const char *log_file_name, my_off_t log_file_pos);

    /**
      Clear the active transaction nodes until(inclusive) the specified
      position.
      If log_file_name is NULL, everything will be cleared: the sorted
      list and the hash table will be reset to empty.

      @return
        @retval  0  success
        @retval -1  error
    */

    int clear_active_tranx_nodes(const char *log_file_name,
                                 my_off_t    log_file_pos);

    /**
      Given a position, check to see whether the position is an active
      transaction's ending position by probing the hash table.
    */

    bool is_tranx_end_pos(const char *log_file_name, my_off_t log_file_pos);

    /**
      Given two binlog positions, compare which one is bigger based on
      (file_name, file_position).
    */

    static int compare(const char *log_file_name1, my_off_t log_file_pos1,
                       const char *log_file_name2, my_off_t log_file_pos2);

  };

  /**
    Active transaction list: the list will be cleared when semi-sync switches
    off.
  */
  Active_tranx   *active_tranxs;

  /** Set when init_object has been called. */
  bool            init_done;

  /**
    This cond variable is signaled when enough binlog has been sent to slave,
    so that a waiting trx can return the 'ok' to the client for a commit.
  */
  pthread_cond_t  COND_binlog_send;

  /**
    Mutex that protects the following state variables and the active
    transaction list.
    Under no circumstances can we acquire mysql_bin_log.LOCK_log if we are
    already holding LOCK_binlog because it can cause deadlocks.
  */
  pthread_mutex_t LOCK_binlog;

  /** This is set to true when reply_file_name contains meaningful data. */
  bool            reply_file_name_inited;

  /** The binlog name up to which we have received replies from any slaves. */
  char            reply_file_name[FN_REFLEN];

  /**
    The position in that file up to which we have the reply from any slaves.
  */
  my_off_t        reply_file_pos;

  /** This is set to true when we know the 'smallest' wait position. */
  bool            wait_file_name_inited;

  /**
    NULL, or the 'smallest' filename that a transaction is waiting for
    slave replies.
  */
  char            wait_file_name[FN_REFLEN];

  /**
    The smallest position in that file that a trx is waiting for: the trx
    can proceed and send an 'ok' to the client when the master has got the
    reply from the slave indicating that it already got the binlog events.
  */
  my_off_t        wait_file_pos;

  /**
    This is set to true when we know the 'largest' transaction commit
    position in the binlog file.
    We always maintain the position no matter whether semi-sync is switched
    on or off. When a transaction wait timeout occurs, semi-sync
    will switch off. Binlog-dump thread can use the following
    three variables to detect when slaves catch up on replication
    so that semi-sync can switch on again.
  */
  bool            commit_file_name_inited;

  /** The 'largest' binlog filename that a commit transaction is seeing. */
  char            commit_file_name[FN_REFLEN];

  /** The 'largest' position in that file that a commit transaction is seeing.*/
  my_off_t        commit_file_pos;

  /* All global variables which can be set by parameters. */
  /** Semi-sync is enabled on the master. */
  bool            master_enabled;
  /** Semi-sync is enabled on the slave. */
  bool            slave_enabled;
  /** Timeout period(ms) during tranx wait. */
  ulong           wait_timeout;
  ulong           trace_level;

  /* All status variables. */
  /** Whether semi-sync is switched on. */
  bool            state;
  ulong           enabled_transactions;       /**< semi-sync'ed transactions */
  /** Number of non-semi-sync'ed transactions. */
  ulong           disabled_transactions;
  ulong           switched_off_times;
  /** How many times gettimeofday failed. */
  ulong           timefunc_fails;
  ulong           total_wait_timeouts;
  /** How many sessions are currently waiting for replies. */
  ulong           wait_sessions;
  /** Number of wait position back traverses. */
  ulong           wait_backtraverse;
  /** Total trx waits: non-timeout ones. */
  ulonglong       total_trx_wait_num;
  ulonglong       total_trx_wait_time;        /**< total trx wait time: in us */
  ulonglong       total_net_wait_num;         /**< total network waits */
  ulonglong       total_net_wait_time;        /**< total network wait time */

  /**
    The number of maximum active transactions. This should be the same as
    maximum connections because MySQL does not do connection sharing now.
  */
  int             max_transactions;

  static const ulong trace_general;
  static const ulong trace_detail;
  static const ulong trace_net_wait;
  static const ulong trace_function;

  static const unsigned char  sync_header[3]; /**< three byte packet header */

  void lock();
  void assert_lock_owner();
  void unlock();
  void cond_broadcast();
  int  cond_timewait(struct timespec *wait_time);

  inline void function_enter(const char *func_name) const;
  inline int  function_exit(const char *func_name, int exit_code) const;

  /**
    Is semi-sync replication on?
  */

  bool is_on() { return (state); }

  void set_master_enabled(bool enabled) { master_enabled= enabled; }

  /**
    Switch semi-sync off because of timeout in transaction
    waiting. Indicate that semi-sync replication is OFF now.

    What should we do when it is disabled? The problem is that we want
    the semi-sync replication enabled again when the slave catches up
    later. But, it is not that easy to detect that the slave has caught
    up. This is caused by the fact that MySQL's replication protocol is
    asynchronous, meaning that if the master does not use the semi-sync
    protocol, the slave would not send anything to the master.
    Still, if the master is sending (N+1)-th event, we assume that it is
    an indicator that the slave has received N-th event and earlier ones.

    If semi-sync is disabled, all transactions still update the wait
    position with the last position in binlog. But no transactions will
    wait for confirmations and the active transaction list would not be
    maintained. In binlog dump thread, update_sync_header() checks whether
    the current sending event catches up with last wait position. If it
    does match, semi-sync will be switched on again.
  */

  int switch_off();

  /** Switch semi-sync on when slaves catch up. */

  int try_switch_on(int server_id,
                    const char *log_file_name, my_off_t log_file_pos);

public:
  Repl_semi_sync();
  ~Repl_semi_sync();

  /* Constants in network packet header. */
  static const unsigned char packet_magic_num;
  static const unsigned char packet_flag_sync;

  bool get_master_enabled() const { return master_enabled; }
  bool get_slave_enabled() const { return slave_enabled; }
  void set_slave_enabled(bool enabled) { slave_enabled= enabled; }

  void set_trace_level(ulong new_trace_level) { trace_level= new_trace_level; }

  /** Set the transaction wait timeout period, in milliseconds. */
  void set_wait_timeout(ulong new_wait_timeout)
  {
    wait_timeout= new_wait_timeout;
  }

  /**
    Initialize this class after MySQL parameters are initialized. this
    function should be called once at bootstrap time.
  */

  int init_object();

  /**
    Enable the object to enable semi-sync replication inside the master.
  */

  int enable_master();

  /**
    Disable semi-sync replication.
  */

  int disable_master();

  /**
    Force semi-sync to be switched on. Used when rpl_semi_sync_always_on
    is set at runtime.
  */

  void force_on() { state= true; }

  /**
    This is called when MySQL writes the binlog entry for the current
    transaction. Saves the latest binlog file name and position in
    the THD struct, which tells where the MySQL binlog entry for
    the current transaction ended.

    @param  thd             binlog-dump thread doing the binlog communication
    @param  log_file_name   binlog file name
    @param  end_offset      the offset in the binlog file up to
                            which we have written to in the master

    @return
      @retval  0  success
      @retval  1  error
  */

  int report_binlog_offset(THD *thd,
                           char *log_file_name,
                           my_off_t end_offset);

  /**
    In semi-sync replication, reports up to which binlog position we have
    received replies from the slaves indicating that at least one
    of them already got the events.

    @param  thd             binlog-dump thread doing the binlog communication
                            to the slave
    @param  log_file_name   binlog file name
    @param  end_offset      the offset in the binlog file up to
                            which we have the replies from the slave

    @return
      @retval  0  success
      @retval -1  error
  */

  int report_reply_binlog(THD *thd, char *log_file_name,
                          my_off_t end_offset);

  /**
    Wait for a semi-sync replication ACK.

    If semi-sync is switched on, the function will wait to see
    whether binlog-dump thread gets the reply for the events of
    the transaction. Remember that this is not a direct wait,
    instead, it waits to see whether the binlog-dump thread has
    reached the point. If the wait times out, semi-sync status
    will be switched off and all other transaction would not wait
    either.

    @param  thd  Thread descriptor, containing the ending
                 position's file name and offset.

    @return
      @retval  0  success
      @retval -1  error
  */

  int commit_trx(THD *thd);

  /**
    Reserve spaces in the replication event packet header:
     . slave semi-sync off: 1 byte - (0)
     . slave semi-sync on:  3 byte - (0, 0xef, 0/1}

    @param  packet               the packet containing the replication event
    @param  thd                  the binlog dump thread
    @param  packet_buffer        the packet's initial buffer
    @param  packet_buffer_size   the size of the initial buffer

    @return
      @retval  0  success
      @retval -1  error
  */

  void reserve_sync_header(String *packet, THD *thd,
                           char *packet_buffer, ulong packet_buffer_size) const;

  /**
    Update the sync bit in the packet header to indicate to the slave whether
    the master will wait for the reply of the event. If semi-sync is switched
    off and we detect that the slave is catching up, we switch semi-sync on.

    @param       packet         the packet containing the replication event
    @param       log_file_name  the event ending position's file name
    @param       log_file_pos   the event ending position's file offset
    @param       thd            the binlog dump thread
    @param[out]  sync           whether the sync bit is set
    @param[out]  event_type     the sending event's type

    @return
      @retval  0  success
      @retval -1  error
  */

  int update_sync_header(String *packet,
                         const char *log_file_name, my_off_t log_file_pos,
                         THD *thd, bool *sync, Log_event_type *event_type);

  /**
    Wait for a slave's reply from the network, and use it to
    update the furthest binlog position replicated.

    @param       thd          binlog dump replication thread
    @param       net          the network socket
    @param[out]  read_errmsg  error message if an error occurs
    @param[out]  read_errno   error number if an error occurs

    @return
      @retval  0  success
      @retval -1  error
  */

  int read_slave_reply(THD *thd, NET *net, const char **read_errmsg,
                       int *read_errno);

  /**
    Called when a transaction finished writing binlog events.
     . update the 'largest' transactions' binlog event position
     . insert the ending position in the active transaction list if
       semi-sync is on

    @param  log_file_name  transaction ending position's file name
    @param  log_file_pos   transaction ending position's file offset

    @return
      @retval  0  success
      @retval -1  error
  */

  int write_tranx_in_binlog(const char *log_file_name, my_off_t log_file_pos);

  /**
    A slave reads the semi-sync packet header and separates the
    metadata from the payload data.

    @param  header       packet header pointer
    @param  total_len    total packet length: metadata + payload
    @param  need_reply   whether the master is waiting for the reply
    @param  payload      the replication event
    @param  payload_len  payload length

    @return
      @retval  0  success
      @retval -1  error
  */

  int slave_read_sync_header(const char *header, ulong total_len,
                             bool *need_reply, const char **payload,
                             ulong *payload_len) const;

  /**
    Sends a slave's replication progress to the master. It
    indicates that the slave has received all events before the
    specified binlog position.

    @param  net              the network socket
    @param  binlog_filename  the reply point's binlog file name
    @param  binlog_filepos   the reply point's binlog file offset

    @return
      @retval  0  success
      @retval -1  error
  */

  int slave_reply(NET *net, const char *binlog_filename,
                  my_off_t binlog_filepos) const;

  /**
    Export internal statistics for semi-sync replication. Caller must lock
    LOCK_status.
  */

  void set_export_stats();

  /**
    Switches off semi-sync replication (unless semi-sync is always on).
    Called in response to the RESET MASTER command, before the binlog is reset.

    Returns with LOCK_binlog locked. Caller must always complete the operation
    by calling reset_master_complete, regardless of return value.

    @param       thd          binlog dump replication thread
    @return
      @retval  0  success
      @retval  1  error
  */

  int reset_master(THD *thd);

  /**
    Called subsequent to reset_master, after the binlog has finished
    resetting.

    Assumes LOCK_binlog is locked. Returns with LOCK_binlog unlocked.

    @return
      @retval  0  success
      @retval  1  error
  */

  int reset_master_complete();
};

#endif                                        /* REPL_SEMI_SYNC_INCLUDED */
