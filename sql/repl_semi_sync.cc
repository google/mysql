/*
  Copyright 2006 Google Inc. All Rights Reserved.
  Author: wei@google.com (Wei Li)

  The file defines two classes that implement semi-sync replication based on
  MySQL's asynchronous replication:
   . Repl_semi_sync::Active_tranx: manage all active transaction nodes
   . Repl_semi_sync: the code flow for semi-sync replication

  By default in semi-sync replication, a transaction waits for 50ms to see
  whether the slave has got the transaction. 50ms is based on the assumption
  that roundtrip time in one data center is less than 1ms and machine
  configurations should make the master database and the semi-sync slave
  database colocate in one data center. Otherwise, "rpl_semi_sync_timeout"
  should be used to adjust timeout value.
*/

#include "repl_semi_sync.h"

#define TIME_THOUSAND 1000
#define TIME_MILLION  1000000
#define TIME_BILLION  1000000000

const unsigned char Repl_semi_sync::packet_magic_num= 0xef;
const unsigned char Repl_semi_sync::packet_flag_sync= 0x01;

const ulong Repl_semi_sync::trace_general=  0x0001;
const ulong Repl_semi_sync::trace_detail=   0x0010;
const ulong Repl_semi_sync::trace_net_wait= 0x0020;
const ulong Repl_semi_sync::trace_function= 0x0040;

const unsigned char Repl_semi_sync::sync_header[3]=
  {0, Repl_semi_sync::packet_magic_num, 0};

Repl_semi_sync semi_sync_replicator;
ulong rpl_event_buffer_size;
ulong rpl_semi_sync_enabled;
my_bool rpl_semi_sync_always_on;
ulong rpl_semi_sync_slave_enabled;
ulong rpl_semi_sync_timeout;
ulong rpl_semi_sync_trace_level;
ulong rpl_semi_sync_status=                  0;
ulong rpl_semi_sync_slave_status=            0;
ulong rpl_semi_sync_yes_transactions=        0;
ulong rpl_semi_sync_no_transactions=         0;
ulong rpl_semi_sync_off_times=               0;
ulong rpl_semi_sync_timefunc_fails=          0;
ulong rpl_semi_sync_num_timeouts=            0;
ulong rpl_semi_sync_wait_sessions=           0;
ulong rpl_semi_sync_back_wait_pos=           0;
ulong rpl_semi_sync_trx_wait_time=           0;
ulong rpl_semi_sync_net_wait_time=           0;
ulonglong rpl_semi_sync_net_wait_num=        0;
ulonglong rpl_semi_sync_trx_wait_num=        0;
ulonglong rpl_semi_sync_net_wait_total_time= 0;
ulonglong rpl_semi_sync_trx_wait_total_time= 0;
ulong rpl_semi_sync_clients=                 0;

static int get_wait_time(const struct timeval * start_tv);

Repl_semi_sync::Active_tranx::Active_tranx(int max_connections,
                                           pthread_mutex_t *in_lock,
                                           ulong *in_trace_level)
  :num_transactions(max_connections),
  num_entries(max_connections << 1),
  lock(in_lock),
  trace_level(in_trace_level)
{
  /* Allocate the memory for the array */
  node_array= new Tranx_node[num_transactions];
  for (int idx= 0; idx < num_transactions; ++idx)
  {
    node_array[idx].log_pos=     0;
    node_array[idx].hash_next=   NULL;
    node_array[idx].next=        node_array + idx + 1;

    node_array[idx].log_name=    new char[FN_REFLEN];
    node_array[idx].log_name[0]= '\x0';
  }
  node_array[num_transactions - 1].next= NULL;

  /* All nodes in the array go to the pool initially. */
  free_pool= node_array;

  /* No transactions are in the list initially. */
  trx_front= NULL;
  trx_rear=  NULL;

  /* Create the hash table to find a transaction's ending event. */
  trx_htb= new Tranx_node *[num_entries];
  for (int idx= 0; idx < num_entries; ++idx)
    trx_htb[idx]= NULL;

  sql_print_information("Semi-sync replication initialized for %d "
                        "transactions.", num_transactions);
}


Repl_semi_sync::Active_tranx::~Active_tranx()
{
  for (int idx= 0; idx < num_transactions; ++idx)
  {
    delete [] node_array[idx].log_name;
    node_array[idx].log_name= NULL;
  }

  delete [] node_array;
  delete [] trx_htb;

  node_array=       NULL;
  trx_htb=          NULL;
  num_transactions= 0;
  num_entries=      0;
}


void Repl_semi_sync::Active_tranx::assert_lock_owner()
{
  safe_mutex_assert_owner(lock);
}


void Repl_semi_sync::Active_tranx::function_enter(const char *func_name)
{
  if ((*trace_level) & trace_function)
    sql_print_information("---> %s enter", func_name);
}


int Repl_semi_sync::Active_tranx::function_exit(const char *func_name,
                                                int exit_code)
{
  if ((*trace_level) & trace_function)
    sql_print_information("<--- %s exit (%d)", func_name, exit_code);
  return exit_code;
}


uint Repl_semi_sync::Active_tranx::calc_hash(const uchar *key, uint length)
{
  uint nr= 1, nr2= 4;

  /* The hash implementation comes from calc_hashnr() in mysys/hash.c. */
  while (length--)
  {
    nr^= (((nr & 63) + nr2) * ((uint) (uchar) *key++)) + (nr << 8);
    nr2+= 3;
  }
  return((uint) nr);
}


uint Repl_semi_sync::Active_tranx::get_hash_value(const char *log_file_name,
                                                  my_off_t    log_file_pos)
{
  uint hash1= calc_hash((const uchar *)log_file_name,
                        strlen(log_file_name));
  uint hash2= calc_hash((const uchar *)(&log_file_pos),
                        sizeof(log_file_pos));

  return (hash1 + hash2) % num_entries;
}


Repl_semi_sync::Active_tranx::Tranx_node*
Repl_semi_sync::Active_tranx::alloc_tranx_node()
{
  Tranx_node *ptr= free_pool;

  if (free_pool)
  {
    free_pool= free_pool->next;
    ptr->next= NULL;
    ptr->hash_next= NULL;
  }

  return ptr;
}


int
Repl_semi_sync::Active_tranx::
compare(const char *log_file_name1, my_off_t log_file_pos1,
        const char *log_file_name2, my_off_t log_file_pos2)
{
  int cmp= strcmp(log_file_name1, log_file_name2);

  if (cmp != 0)
    return cmp;

  if (log_file_pos1 > log_file_pos2)
    return 1;
  else if (log_file_pos1 < log_file_pos2)
    return -1;
  return 0;
}


int Repl_semi_sync::Active_tranx::insert_tranx_node(const char *log_file_name,
                                                    my_off_t log_file_pos)
{
  const char *who= "Active_tranx:insert_tranx_node";
  Tranx_node *ins_node;
  int         result= 0;
  uint        hash_val;

  function_enter(who);
  assert_lock_owner();

  ins_node= alloc_tranx_node();
  if (!ins_node)
  {
    sql_print_error("%s: transaction node allocation failed for: (%s, %lu)",
                    who, log_file_name, (ulong)log_file_pos);
    result= -1;
    goto end;
  }

  /* insert the binlog position in the active transaction list. */
  strncpy(ins_node->log_name, log_file_name, FN_REFLEN);
  ins_node->log_pos= log_file_pos;

  if (!trx_front)
  {
    /* The list is empty. */
    trx_front= trx_rear= ins_node;
  }
  else
  {
    int cmp= compare(ins_node, trx_rear);
    if (cmp > 0)
    {
      /*
        Compare with the tail first. If the transaction happens later in
        binlog, then make it the new tail.
      */
      trx_rear->next= ins_node;
      trx_rear=       ins_node;
    }
    else
    {
      /*
        Otherwise, it is an error because the transaction should hold the
        mysql_bin_log.LOCK_log when appending events.
      */
      char buf1[22], buf2[22];
      sql_print_error("%s: binlog write out-of-order, tail (%s, %s), "
                      "new node (%s, %s)", who,
                      trx_rear->log_name,
                      llstr((longlong)trx_rear->log_pos, buf1),
                      ins_node->log_name,
                      llstr((longlong)ins_node->log_pos, buf2));
      result= -1;
      goto end;
    }
  }

  hash_val= get_hash_value(ins_node->log_name, ins_node->log_pos);
  ins_node->hash_next= trx_htb[hash_val];
  trx_htb[hash_val]=   ins_node;

  if ((*trace_level) & trace_detail)
    sql_print_information("%s: insert (%s, %lu) in entry(%u)", who,
                          ins_node->log_name, (ulong)ins_node->log_pos,
                          hash_val);

end:
  return function_exit(who, result);
}


bool Repl_semi_sync::Active_tranx::is_tranx_end_pos(const char *log_file_name,
                                                    my_off_t    log_file_pos)
{
  const char *who= "Repl_semi_sync::is_tranx_end_pos";
  function_enter(who);

  uint hash_val= get_hash_value(log_file_name, log_file_pos);
  Tranx_node *entry= trx_htb[hash_val];

  assert_lock_owner();
  while (entry != NULL)
  {
    if (compare(entry, log_file_name, log_file_pos) == 0)
      break;
    entry= entry->hash_next;
  }

  if ((*trace_level) & trace_detail)
    sql_print_information("%s: probe (%s, %lu) in entry(%u)", who,
                          log_file_name, (ulong)log_file_pos, hash_val);

  function_exit(who, (entry != NULL));
  return (entry != NULL);
}


int
Repl_semi_sync::Active_tranx::
clear_active_tranx_nodes(const char *log_file_name,
                         my_off_t log_file_pos)
{
  const char *who= "Active_tranx::clear_active_tranx_nodes";
  Tranx_node *new_front;

  function_enter(who);

  /* Must hold the lock during the call. */
  assert_lock_owner();

  if (log_file_name != NULL)
  {
    new_front= trx_front;

    while (new_front)
    {
      if (compare(new_front, log_file_name, log_file_pos) > 0)
        break;
      new_front= new_front->next;
    }
  }
  else
  {
    /* If log_file_name is NULL, clear everything. */
    new_front= NULL;
  }

  if (new_front == NULL)
  {
    /* No active transaction nodes after the call. */

    /* Clear the hash table. */
    memset(trx_htb, 0, num_entries * sizeof(Tranx_node *));

    /* Clear the active transaction list. */
    if (trx_front != NULL)
    {
      trx_rear->next= free_pool;
      free_pool= trx_front;
      trx_front= NULL;
      trx_rear=  NULL;
    }

    if ((*trace_level) & trace_detail)
      sql_print_information("%s: free all nodes back to free list", who);
  }
  else if (new_front != trx_front)
  {
    Tranx_node *curr_node, *next_node;

    /* Delete all transaction nodes before the confirmation point. */
    int n_frees= 0;
    curr_node= trx_front;
    while (curr_node != new_front)
    {
      next_node= curr_node->next;

      /* Put the node in the memory pool. */
      curr_node->next= free_pool;
      free_pool=       curr_node;
      n_frees++;

      /* Remove the node from the hash table. */
      uint hash_val= get_hash_value(curr_node->log_name, curr_node->log_pos);
      Tranx_node **hash_ptr= &(trx_htb[hash_val]);
      while ((*hash_ptr) != NULL)
      {
        if ((*hash_ptr) == curr_node)
        {
          (*hash_ptr)= curr_node->hash_next;
          break;
        }
        hash_ptr= &((*hash_ptr)->hash_next);
      }

      curr_node= next_node;
    }

    trx_front= new_front;

    if ((*trace_level) & trace_detail)
    {
      char buf[22];
      sql_print_information("%s: free %d nodes back until pos (%s, %s)",
                            who, n_frees,
                            trx_front->log_name,
                            llstr((longlong)trx_front->log_pos, buf));
    }
  }

  return function_exit(who, 0);
}


Repl_semi_sync::Repl_semi_sync()
  :active_tranxs(NULL),
  init_done(false),
  reply_file_name_inited(false),
  reply_file_pos(0L),
  wait_file_name_inited(false),
  wait_file_pos(0),
  master_enabled(false),
  slave_enabled(false),
  wait_timeout(0L),
  trace_level(0L),
  state(0),
  enabled_transactions(0),
  disabled_transactions(0),
  switched_off_times(0),
  timefunc_fails(0),
  total_wait_timeouts(0),
  wait_sessions(0),
  wait_backtraverse(0),
  total_trx_wait_num(0),
  total_trx_wait_time(0),
  total_net_wait_num(0),
  total_net_wait_time(0),
  max_transactions(0L)
{
  reply_file_name[0]= '\0';
  wait_file_name[0]=  '\0';
}


int Repl_semi_sync::init_object()
{
  int result;
  const char *who= "Repl_semi_sync::init_object";

  if (init_done)
  {
    // Use fprintf because sql_print_error might not be initialized.
    fprintf(stderr, "%s called twice\n", who);
    unireg_abort(1);
  }
  init_done= true;

  /* References to the parameter works after set_options(). */
  set_slave_enabled(rpl_semi_sync_slave_enabled);
  set_wait_timeout(rpl_semi_sync_timeout);
  set_trace_level(rpl_semi_sync_trace_level);
  max_transactions= (int)max_connections;

  /* Mutex initialization can only be done after MY_INIT(). */
  pthread_mutex_init(&LOCK_binlog, MY_MUTEX_INIT_FAST);
  pthread_cond_init(&COND_binlog_send, NULL);

  if (rpl_semi_sync_enabled)
    result= enable_master();
  else
    result= disable_master();

  return result;
}


int Repl_semi_sync::enable_master()
{
  int result= 0;

  /* Must have the lock when we do enable or disable. */
  lock();

  if (!get_master_enabled())
  {
    DBUG_ASSERT(active_tranxs == NULL);
    active_tranxs= new Repl_semi_sync::Active_tranx(max_connections,
                                                    &LOCK_binlog,
                                                    &trace_level);
    if (active_tranxs != NULL)
    {
      reply_file_name_inited=  false;
      wait_file_name_inited=   false;
      commit_file_name_inited= false;

      set_master_enabled(true);
      sql_print_information("Semi-sync replication enabled on the master.");
    }
    else
    {
      sql_print_error("Semi-sync replication not able to allocate memory.");
      result= -1;
    }

    if (rpl_semi_sync_always_on)
      state= true;
  }

  unlock();

  return result;
}


int Repl_semi_sync::disable_master()
{
  /* Must have the lock when we do enable or disable. */
  lock();

  if (get_master_enabled())
  {
    bool reply_file_name_was_inited= reply_file_name_inited;
    /*
      Switch off the semi-sync first so that waiting transaction will be
      waken up.
    */
    switch_off();

    DBUG_ASSERT(active_tranxs != NULL);
    delete active_tranxs;
    active_tranxs= NULL;

    reply_file_name_inited=  false;
    wait_file_name_inited=   false;
    commit_file_name_inited= false;

    set_master_enabled(false);
    sql_print_information("Semi-sync replication disabled on the master.");
    if (reply_file_name_was_inited)
      sql_print_information("Semi-sync up to file %s, position %lu",
                            reply_file_name, (ulong)reply_file_pos);
    else
      sql_print_information("Master did not receive any semi-sync ACKs.");
  }

  unlock();

  return 0;
}


Repl_semi_sync::~Repl_semi_sync()
{
  if (init_done)
  {
    pthread_mutex_destroy(&LOCK_binlog);
    pthread_cond_destroy(&COND_binlog_send);
  }

  delete active_tranxs;
}


void Repl_semi_sync::lock()
{
  pthread_mutex_lock(&LOCK_binlog);
}


void Repl_semi_sync::assert_lock_owner()
{
  safe_mutex_assert_owner(&LOCK_binlog);
}


void Repl_semi_sync::unlock()
{
  pthread_mutex_unlock(&LOCK_binlog);
}


void Repl_semi_sync::cond_broadcast()
{
  pthread_cond_broadcast(&COND_binlog_send);
}


int Repl_semi_sync::cond_timewait(struct timespec *wait_time)
{
  const char *who= "Repl_semi_sync::cond_timewait()";
  int wait_res;

  function_enter(who);
  wait_res= pthread_cond_timedwait(&COND_binlog_send,
                                   &LOCK_binlog, wait_time);
  return function_exit(who, wait_res);
}


void Repl_semi_sync::function_enter(const char *func_name) const
{
  if (trace_level & trace_function)
    sql_print_information("---> %s enter", func_name);
}


int Repl_semi_sync::function_exit(const char *func_name, int exit_code) const
{
  if (trace_level & trace_function)
    sql_print_information("<--- %s exit (%d)", func_name, exit_code);
  return exit_code;
}


int Repl_semi_sync::report_binlog_offset(THD *thd,
                                         char *log_file_name,
                                         my_off_t end_offset)
{
  int error= 0;

  if (get_master_enabled())
  {
    /*
      Let us store the binlog file name and the position, so that
      we know how long to wait for the binlog to be replicated to
      the slave in synchronous replication.
    */

    if (thd->repl_wait_binlog_name == NULL)
      thd->repl_wait_binlog_name= (char*)my_malloc(FN_REFLEN,
                                                   MYF(MY_FAE | MY_WME));

    DBUG_ASSERT(strlen(log_file_name) < FN_REFLEN);
    strncpy(thd->repl_wait_binlog_name,
            log_file_name + dirname_length(log_file_name),
            FN_REFLEN);
    thd->repl_wait_binlog_pos= end_offset;

    /* Store transaction ending position for semi-sync replication. */
    error= write_tranx_in_binlog(thd->repl_wait_binlog_name, end_offset);
    if (error)
    {
      my_error(ER_ERROR_DURING_COMMIT, MYF(0), error);
      error= 1;
    }
  }

  return error;
}


int Repl_semi_sync::report_reply_binlog(THD      *thd,
                                        char     *log_file_name,
                                        my_off_t  log_file_pos)
{
  const char *who= "Repl_semi_sync::report_reply_binlog";
  int   cmp;
  bool  can_release_threads= false;
  bool  need_copy_send_pos= true;

  LOG_INFO linfo;

  /*
    If semi-sync replication is not enabled, or this thd is
    sending binlog to a slave where we do not need synchronous replication,
    then return immediately.
  */
  if (!(get_master_enabled() && thd->semi_sync_slave))
  {
    return 0;
  }

  function_enter(who);

  /*
    Sanity check the log and pos from the reply. If it is from the 'future'
    then the slave is horked in some way or the packet was corrupted
    on the network. In either case we should ignore the reply.

    NOTE: get_current_log acquires LOCK_log so must be called prior to
    calling lock() to avoid a deadlock between this thread and a thread
    inside Repl_semi_sync::write_tranx_in_binlog which also acquires both
    locks.
  */
  if (mysql_bin_log.get_current_log(&linfo))
  {
    sql_print_error("Repl_semi_sync::report_reply_binlog failed to read "
                    "current binlog position for sanity check.");
    goto end;
  }

  lock();

  /* This is the real check inside the mutex. */
  if (!get_master_enabled())
    goto end;

  if (Active_tranx::compare(log_file_name, log_file_pos,
                            base_name(linfo.log_file_name), linfo.pos) > 0)
  {
    sql_print_error("Bad semi-sync reply received from %s: "
                    "reply position (%s, %lu), "
                    "current binlog position (%s, %lu).",
                    thd->security_ctx->host_or_ip,
                    log_file_name, (ulong) log_file_pos,
                    base_name(linfo.log_file_name), (ulong) linfo.pos);
    goto end;
  }

  if (!is_on())
  {
    /* We check to see whether we can switch semi-sync ON. */
    try_switch_on(thd->server_id, log_file_name, log_file_pos);
  }

  /*
    The position should increase monotonically, if there is only one
    thread sending the binlog to the slave.
    In reality, to improve the transaction availability, we allow multiple
    sync replication slaves. So, if any one of them get the transaction,
    the transaction session in the primary can move forward.
  */
  if (reply_file_name_inited)
  {
    cmp= Active_tranx::compare(log_file_name, log_file_pos,
                               reply_file_name, reply_file_pos);

    /*
      If the requested position is behind that of another slave we don't
      need to track this slave's position.
      We work on the assumption that there are multiple semi-sync slaves,
      and at least one of them should be up to date.
      If all semi-sync slaves are behind, at least initially, the primary
      can find the situation after the waiting timeout. After that, some
      slaves should catch up quickly.
    */
    if (cmp < 0)
    {
      /* If the position is behind, do not copy it. */
      need_copy_send_pos= false;
    }
  }

  if (need_copy_send_pos)
  {
    strncpy(reply_file_name, log_file_name, FN_REFLEN);
    reply_file_pos= log_file_pos;
    reply_file_name_inited= true;

    /* Remove all active transaction nodes before this point. */
    DBUG_ASSERT(active_tranxs != NULL);
    active_tranxs->clear_active_tranx_nodes(log_file_name, log_file_pos);

    if (trace_level & trace_detail)
      sql_print_information("%s: Got reply at (%s, %lu)", who,
                            log_file_name, (ulong)log_file_pos);
  }

  if (wait_sessions > 0)
  {
    /*
      Let us check if some of the waiting threads doing a trx
      commit can now proceed.
    */
    cmp= Active_tranx::compare(reply_file_name, reply_file_pos,
                               wait_file_name, wait_file_pos);
    if (cmp >= 0)
    {
      /*
        Yes, at least one waiting thread can now proceed:
        let us release all waiting threads with a broadcast
      */
      can_release_threads= true;
      wait_file_name_inited= false;
    }
  }

end:
  unlock();

  if (can_release_threads)
  {
    if (trace_level & trace_detail)
      sql_print_information("%s: signal all waiting threads.", who);

    cond_broadcast();
  }

  return function_exit(who, 0);
}


int Repl_semi_sync::commit_trx(THD *thd)
{
  const char *who= "Repl_semi_sync::commit_trx";

  function_enter(who);
  const char *trx_wait_binlog_name= thd->repl_wait_binlog_name;
  my_off_t trx_wait_binlog_pos= thd->repl_wait_binlog_pos;
  int error= 0;

  if (get_master_enabled() &&
      trx_wait_binlog_name && strlen(trx_wait_binlog_name) > 0)
  {
    struct timeval start_tv;
    struct timespec abstime;
    int wait_result, start_time_err;

    bool use_exit_cond= false;
    const char *old_msg= NULL;

    start_time_err= gettimeofday(&start_tv, 0);

    /* Acquire the mutex. */
    lock();

    /* This is the real check inside the mutex. */
    if (!get_master_enabled())
      goto end;

    if (trace_level & trace_detail)
    {
      sql_print_information("%s: wait pos (%s, %lu), repl(%d)\n", who,
                            trx_wait_binlog_name, (ulong)trx_wait_binlog_pos,
                            (int)is_on());
    }

    while (is_on())
    {
      int cmp= Active_tranx::compare(reply_file_name, reply_file_pos,
                                     trx_wait_binlog_name, trx_wait_binlog_pos);
      if (cmp >= 0)
      {
        /*
          We have already sent the relevant binlog to the slave: no need to
          wait here.
        */
        if (trace_level & trace_detail)
          sql_print_information("%s: Binlog reply is ahead (%s, %lu),",
                                who, reply_file_name, (ulong)reply_file_pos);
        break;
      }

      /*
        Let us update the info about the minimum binlog position of waiting
        threads.
      */
      if (wait_file_name_inited)
      {
        cmp= Active_tranx::compare(trx_wait_binlog_name, trx_wait_binlog_pos,
                                   wait_file_name, wait_file_pos);
        if (cmp <= 0)
        {
          /* This thd has a lower position, let's update the minimum info. */
          strncpy(wait_file_name, trx_wait_binlog_name, FN_REFLEN);
          wait_file_pos= trx_wait_binlog_pos;

          wait_backtraverse++;
          if (trace_level & trace_detail)
            sql_print_information("%s: move back wait position (%s, %lu),",
                                  who, wait_file_name, (ulong)wait_file_pos);
        }
      }
      else
      {
        strncpy(wait_file_name, trx_wait_binlog_name, FN_REFLEN);
        wait_file_pos= trx_wait_binlog_pos;
        wait_file_name_inited= true;

        if (trace_level & trace_detail)
          sql_print_information("%s: init wait position (%s, %lu),",
                                who, wait_file_name, (ulong)wait_file_pos);
      }

      if (start_time_err == 0)
      {
        /* Calculate the waiting period. */
        ulong carry= 0;
        ulong wait_timeout_whole_secs= wait_timeout / TIME_THOUSAND;
        ulong wait_timeout_milli_secs= wait_timeout % TIME_THOUSAND;

        abstime.tv_nsec= ((start_tv.tv_usec +
                          (wait_timeout_milli_secs * TIME_THOUSAND)) *
                          TIME_THOUSAND);
        if (abstime.tv_nsec >= TIME_BILLION)
        {
          abstime.tv_nsec-= TIME_BILLION;
          carry= 1;
        }
        abstime.tv_sec= carry + start_tv.tv_sec + wait_timeout_whole_secs;

        /*
          In semi-synchronous replication, we wait until the binlog-dump
          thread has received the reply on the relevant binlog segment from the
          replication slave.

          Let us suspend this thread to wait on the condition;
          when replication has progressed far enough, we will release
          these waiting threads.
        */
        wait_sessions++;

        if (trace_level & trace_detail)
          sql_print_information("%s: wait %lu ms for binlog sent (%s, %lu)",
                                who, wait_timeout,
                                wait_file_name, (ulong)wait_file_pos);

        DBUG_PRINT("info", ("Waiting for binlog to be sent"));
        use_exit_cond= true;
        old_msg= thd->enter_cond(&COND_binlog_send, &LOCK_binlog,
                                 "Waiting to send binlog to semi-sync slave.");
        wait_result= cond_timewait(&abstime);
        wait_sessions--;

        /*
          If the condition was signaled because the THD is being killed,
          treat it as a timeout. Note that there's a small chance that this
          isn't correct. The condition could have been signaled because a
          slave received the transaction, a context switch occurred which marked
          this THD as killed, context switch back to here and thd->killed gets
          checked and we incorrectly mark it as a timeout. Not a big deal
          though because either way the client is going to get an error.
        */
        if (wait_result != 0 || thd->killed)
        {
          char msg[256];
          my_snprintf(msg, sizeof(msg),
                      "%s while waiting for replication semi-sync ack.",
                      (thd->killed ? "Killed" : "Timeout"));
          sql_print_information("%s", msg);

          if (thd->killed)
          {
            /* Return error to client. */
            error= 1;
            my_printf_error(ER_ERROR_DURING_COMMIT, "%s", MYF(0), msg);
          }

          if (trace_level & trace_general)
          {
            sql_print_information("Replication semi-sync did not send binlog to"
                                  " slave within the timeout %lu ms - OFF.",
                                  wait_timeout);
            sql_print_information("  Semi-sync up to file %s, position %lu",
                                  reply_file_name, (ulong)reply_file_pos);
            sql_print_information("  Transaction needs file %s, position %lu",
                                  trx_wait_binlog_name,
                                  (ulong)trx_wait_binlog_pos);
          }

          /* For stat purposes, killed THDs are lumped in with timeouts. */
          total_wait_timeouts++;

          /* Switch semi-sync off. */
          if (!rpl_semi_sync_always_on)
            switch_off();
          else
          {
            if (trace_level & trace_general)
              sql_print_information("Semi-sync always-on enabled (1): skipping "
                                    "switch_off.");
            break;
          }
        }
        else
        {
          int wait_time;

          wait_time= get_wait_time(&start_tv);
          if (wait_time < 0)
          {
            char msg[256];
            my_snprintf(msg, sizeof(msg),
                        "Replication semi-sync gettimeofday fail (1) at "
                        "wait position (%s, %d)",
                        trx_wait_binlog_name, (uint)trx_wait_binlog_pos);
            sql_print_information("%s", msg);

            /* Return error to client. */
            error= 1;
            my_printf_error(ER_ERROR_DURING_COMMIT, "%s", MYF(0), msg);
            timefunc_fails++;
          }
          else
          {
            total_trx_wait_num++;
            total_trx_wait_time+= wait_time;
          }
        }
      }
      else
      {
        char msg[256];
        my_snprintf(msg, sizeof(msg),
                    "Replication semi-sync gettimeofday fail (2) at "
                    "wait position (%s, %d)",
                    trx_wait_binlog_name, (uint)trx_wait_binlog_pos);
        sql_print_information("%s", msg);

        /* Return error to client. */
        error= 1;
        my_printf_error(ER_ERROR_DURING_COMMIT, "%s", MYF(0), msg);
        timefunc_fails++;

        /* Switch semi-sync off. */
        if (!rpl_semi_sync_always_on)
          switch_off();
        else
        {
          if (trace_level & trace_general)
            sql_print_information("Semi-sync always-on enabled (2): skipping "
                                  "switch_off.");
          break;
        }
      }
    }

end:
    /* Update the status counter. */
    if (is_on())
      enabled_transactions++;
    else
      disabled_transactions++;

    if (use_exit_cond)
      thd->exit_cond(old_msg);
    else
      unlock();

    thd->repl_wait_binlog_name[0]= '\0';
    thd->repl_wait_binlog_pos= 0;
  }

  return function_exit(who, error);
}


int Repl_semi_sync::switch_off()
{
  const char *who= "Repl_semi_sync::switch_off";
  int result;

  function_enter(who);
  assert_lock_owner();
  state= false;

  /* Clear the active transaction list. */
  DBUG_ASSERT(active_tranxs != NULL);
  result= active_tranxs->clear_active_tranx_nodes(NULL, 0);

  switched_off_times++;
  wait_file_name_inited=   false;
  reply_file_name_inited=  false;
  commit_file_name_inited= false;
  cond_broadcast();                           /* wake up all waiting threads */

  return function_exit(who, result);
}


int Repl_semi_sync::try_switch_on(int server_id, const char *log_file_name,
                                  my_off_t log_file_pos)
{
  const char *who= "Repl_semi_sync::try_switch_on";
  bool semi_sync_on= false;

  function_enter(who);
  assert_lock_owner();

  /*
    If the current sending event's position is larger than or equal to the
    'largest' commit transaction binlog position, the slave is already
    catching up now and we can switch semi-sync on here.
    If commit_file_name_inited_ indicates there are no recent transactions,
    we can enable semi-sync immediately.
  */
  if (commit_file_name_inited)
  {
    int cmp= Active_tranx::compare(log_file_name, log_file_pos,
                                   commit_file_name, commit_file_pos);
    semi_sync_on= (cmp >= 0);
  }
  else
  {
    semi_sync_on= true;
  }

  if (semi_sync_on)
  {
    /* Switch semi-sync replication on. */
    state= true;

    if (trace_level & trace_general)
      sql_print_information("%s switch semi-sync ON with server(%d) "
                            "at (%s, %lu), repl(%d)",
                            who, server_id, log_file_name,
                            (ulong)log_file_pos, (int)is_on());
  }

  return function_exit(who, 0);
}


void Repl_semi_sync::reserve_sync_header(String *packet, THD *thd,
                                         char *packet_buffer,
                                         ulong packet_buffer_size) const
{
  const char *who= "Repl_semi_sync::reserve_sync_header";
  function_enter(who);

  packet->set(packet_buffer, (uint32)packet_buffer_size, &my_charset_bin);
  packet->length(0);
  if (!thd->semi_sync_slave)
  {
    packet->append("\0", 1);
  }
  else
  {
    /*
      Set the magic number and the sync status. By default, no sync
      is required.
    */
    packet->append(reinterpret_cast<const char*>(sync_header),
                   sizeof(sync_header));
  }
  function_exit(who, 0);
}


int Repl_semi_sync::update_sync_header(String         *packet,
                                       const char     *log_file_name,
                                       my_off_t        log_file_pos,
                                       THD            *thd,
                                       bool           *sync,
                                       Log_event_type *event_type)
{
  const char *who= "Repl_semi_sync::update_sync_header";
  int  cmp= 0;

  /*
    If the semi-sync master is not enabled, or the slave is not a semi-sync
    target, do not request replies from the slave.
  */
  if (!get_master_enabled() || !thd->semi_sync_slave)
  {
    int ev_offset= thd->semi_sync_slave ? 3 : 1;
    *sync=         false;

    *event_type= (Log_event_type)((*packet)[LOG_EVENT_OFFSET + ev_offset]);
    return 0;
  }

  function_enter(who);

  lock();

  /* This is the real check inside the mutex. */
  if (!get_master_enabled())
  {
    *sync= false;
    goto end;
  }

  if (is_on())                                /* semi-sync is ON */
  {
    /* No sync unless a transaction is involved. */
    *sync= false;

    if (reply_file_name_inited)
    {
      cmp= Active_tranx::compare(log_file_name, log_file_pos,
                                 reply_file_name, reply_file_pos);
      if (cmp <= 0)
      {
        /*
          If we have already got the reply for the event, then we do
          not need to sync the transaction again.
        */
        cmp= -1;
      }
    }

    if (cmp >= 0)
    {
      if (wait_file_name_inited)
        cmp= Active_tranx::compare(log_file_name, log_file_pos,
                                   wait_file_name, wait_file_pos);
      else
        cmp= 1;

      if (cmp >= 0)
      {
        /*
          We are going to send an event which has not reached the final
          commit point inside InnoDB.
          We need the reply from the slave because soon the transaction
          should wait for the reply when it reaches the end of the
          commit.

          We only wait if the event is a transaction's ending event.
        */
        DBUG_ASSERT(active_tranxs != NULL);
        *sync= active_tranxs->is_tranx_end_pos(log_file_name, log_file_pos);
      }
      else
      {
        /*
          If we are already waiting for some transaction replies which
          are later in binlog, do not wait for this one event.
        */
      }
    }
  }
  else                                        /* semi-sync is OFF */
  {
    /* Check to see whether we can switch semi-sync ON. */
    try_switch_on(thd->server_id, log_file_name, log_file_pos);

    /*
      We must request sync reply for the current event no matter whether it
      is the end of a transaction.
      Here is the problematic situation:
       . write_tranx_in_binlog(): update commit_file_* base on transaction-A
       . update_sync_header(): switch on semi-sync replication
       . commit_trx(): we would wait until timeout for transaction-A for
                       which binlog_dump thread never requests replies

      Also, it is advantageous that we update commit_file_* inside function
      write_tranx_in_binlog(). Because commit_file_* indicates the last
      transaction in binlog and the current event must be equal or behind
      the last transaction, a reply to the current event from the slave
      can clear all older transactions' syncness.
    */
    *sync= is_on();
  }

  if (trace_level & trace_detail)
    sql_print_information("%s: server(%d), (%s, %lu) sync(%d), repl(%d)",
                          who, thd->server_id, log_file_name,
                          (ulong)log_file_pos, *sync, (int)is_on());

end:
  unlock();

  *event_type= (Log_event_type)((*packet)[LOG_EVENT_OFFSET + 3]);

  /*
    Because of the complexity of processing LOAD DATA events, don't ask for
    a sync after one.
  */
  if (*event_type == LOAD_EVENT)
    *sync= false;

  /*
    We do not need to clear sync flag because we set it to 0 when we
    reserve the packet header.
  */
  if (*sync)
    (packet->c_ptr())[2]= packet_flag_sync;

  return function_exit(who, 0);
}


int Repl_semi_sync::read_slave_reply(THD *thd, NET *net,
                                     const char **read_errmsg,
                                     int *read_errno)
{
  const char *who= "Repl_semi_sync::read_slave_reply";
  const unsigned char *packet;
  char     log_file_name[FN_REFLEN];
  my_off_t log_file_pos;
  ulong    packet_len;
  int      result= -1;

  struct timeval start_tv;
  int   start_time_err;
  ulong trc_level= trace_level;

  function_enter(who);

  if (trc_level & trace_net_wait)
    start_time_err= gettimeofday(&start_tv, 0);

  /*
    We flush to make sure that the current event is sent to the network,
    instead of being buffered in the TCP/IP stack.
  */
  if (net_flush(net))
  {
    *read_errmsg= "failed on net_flush()";
    *read_errno=  ER_UNKNOWN_ERROR;
    goto end;
  }

  if (trc_level & trace_detail)
    sql_print_information("%s: Wait for replica's reply", who);

  /*
    Wait for the network here. Though binlog dump thread can indefinitely wait
    here, transactions would not wait indefinitely.
    Transactions wait on binlog replies detected by binlog dump threads. If
    binlog dump threads wait too long, transactions will timeout and continue.
  */
  packet_len= my_net_read(net);

  if (trc_level & trace_net_wait)
  {
    if (start_time_err != 0)
    {
      sql_print_error("Network wait gettimeofday fail1");
      timefunc_fails++;
    }
    else
    {
      int wait_time;

      wait_time= get_wait_time(&start_tv);
      if (wait_time < 0)
      {
        sql_print_error("Network wait gettimeofday fail2");
        timefunc_fails++;
      }
      else
      {
        total_net_wait_num++;
        total_net_wait_time+= wait_time;
      }
    }
  }

  if (packet_len == packet_error || packet_len < 9)
  {
    if (packet_len == packet_error)
      *read_errmsg= "Read semi-sync reply network error";
    else
      *read_errmsg= "Read semi-sync reply length error";
    *read_errno= ER_UNKNOWN_ERROR;
    goto end;
  }

  packet= net->read_pos;
  if (packet[0] != Repl_semi_sync::packet_magic_num)
  {
    *read_errmsg= "Read semi-sync reply magic number error";
    *read_errno=  ER_UNKNOWN_ERROR;
    goto end;
  }

  log_file_pos= uint8korr(packet + 1);
  packet+= 9;
  if (*packet == Repl_semi_sync::packet_magic_num)
    ++packet;
  strncpy(log_file_name, (const char*)packet, FN_REFLEN);

  if (trc_level & trace_detail)
    sql_print_information("%s: Got reply (%s, %lu)",
                          who, log_file_name, (ulong)log_file_pos);

  result= report_reply_binlog(thd, log_file_name, log_file_pos);

end:
  return function_exit(who, result);
}


int Repl_semi_sync::write_tranx_in_binlog(const char *log_file_name,
                                          my_off_t    log_file_pos)
{
  const char *who= "Repl_semi_sync::write_tranx_in_binlog";
  int result= 0;

  function_enter(who);

  lock();

  /* This is the real check inside the mutex. */
  if (!get_master_enabled())
    goto end;

  /*
    Update the 'largest' transaction commit position seen so far even
    though semi-sync is switched off.
    It is much better that we update commit_file_* here, instead of
    inside commit_trx(). This is mostly because update_sync_header()
    will watch for commit_file_* to decide whether to switch semi-sync
    on. The detailed reason is explained in function update_sync_header().
  */
  if (commit_file_name_inited)
  {
    int cmp= Active_tranx::compare(log_file_name, log_file_pos,
                                   commit_file_name, commit_file_pos);
    if (cmp > 0)
    {
      /* This is a larger position, let's update the maximum info. */
      strncpy(commit_file_name, log_file_name, FN_REFLEN);
      commit_file_pos= log_file_pos;
    }
  }
  else
  {
    strncpy(commit_file_name, log_file_name, FN_REFLEN);
    commit_file_pos= log_file_pos;
    commit_file_name_inited= true;
  }

  if (is_on())
  {
    DBUG_ASSERT(active_tranxs != NULL);
    result= active_tranxs->insert_tranx_node(log_file_name, log_file_pos);
  }

end:
  unlock();

  return function_exit(who, result);
}


int Repl_semi_sync::slave_read_sync_header(const char *header,
                                           ulong total_len,
                                           bool *need_reply,
                                           const char **payload,
                                           ulong *payload_len) const
{
  const char *who= "Repl_semi_sync::slave_read_sync_header";
  int read_res= 0;
  function_enter(who);

  if ((unsigned char)(header[0]) == packet_magic_num)
  {
    *need_reply=  (header[1] & packet_flag_sync);
    *payload_len= total_len - 2;
    *payload=     header + 2;

    if (trace_level & trace_detail)
      sql_print_information("%s: reply - %d", who, *need_reply);
  }
  else
  {
    sql_print_error("Missing magic number for semi-sync packet, packet "
                    "len: %d", (int)total_len);
    read_res= -1;
  }

  return function_exit(who, read_res);
}


int Repl_semi_sync::slave_reply(NET        *net,
                                const char *binlog_filename,
                                my_off_t    binlog_filepos) const
{
  const char *who= "Repl_semi_sync::slave_reply";
  uchar reply_buffer[1 + 8 + 1 + FN_REFLEN];
  int   reply_res, name_len= strlen(binlog_filename);

  function_enter(who);

  /* Prepare the buffer of the reply. */
  reply_buffer[0]= packet_magic_num;
  int8store(reply_buffer + 1, binlog_filepos);
  reply_buffer[9]= packet_magic_num;
  memcpy(reply_buffer + 10, binlog_filename, name_len);

  if (trace_level & trace_detail)
    sql_print_information("%s: reply (%s, %lu)", who,
                          binlog_filename, (ulong)binlog_filepos);

  /* Send the reply. */
  reply_res= my_net_write(net, reply_buffer, name_len + 10);
  if (reply_res == 0)
    reply_res= net_flush(net);

  return function_exit(who, reply_res);
}


int Repl_semi_sync::reset_master(THD *thd)
{
  const char *who= "Repl_semi_sync::reset_master";

  function_enter(who);

  lock();

  /* If semi-sync is ON, switch it OFF. */
  int result= 0;
  if (get_master_enabled() && is_on())
  {
    result= switch_off();
  }

  return function_exit(who, result);
}


int Repl_semi_sync::reset_master_complete()
{
  const char *who= "Repl_semi_sync::reset_master_complete";

  function_enter(who);
  assert_lock_owner();

  if (rpl_semi_sync_always_on)
    state= true;

  unlock();

  return function_exit(who, 0);
}


void Repl_semi_sync::set_export_stats()
{
  safe_mutex_assert_owner(&LOCK_status);
  lock();

  rpl_semi_sync_status=           state ? 1 : 0;
  rpl_semi_sync_yes_transactions= enabled_transactions;
  rpl_semi_sync_no_transactions=  disabled_transactions;
  rpl_semi_sync_off_times=        switched_off_times;
  rpl_semi_sync_timefunc_fails=   timefunc_fails;
  rpl_semi_sync_num_timeouts=     total_wait_timeouts;
  rpl_semi_sync_wait_sessions=    wait_sessions;
  rpl_semi_sync_back_wait_pos=    wait_backtraverse;
  rpl_semi_sync_trx_wait_num=     total_trx_wait_num;
  rpl_semi_sync_trx_wait_time=
    ((total_trx_wait_num) ?
     (ulong)((double)total_trx_wait_time /
             ((double)total_trx_wait_num)) : 0);
  rpl_semi_sync_net_wait_num=     total_net_wait_num;
  rpl_semi_sync_net_wait_time=
    ((total_net_wait_num) ?
     (ulong)((double)total_net_wait_time /
             ((double)total_net_wait_num)) : 0);

  rpl_semi_sync_net_wait_total_time= total_net_wait_time;
  rpl_semi_sync_trx_wait_total_time= total_trx_wait_time;

  unlock();
}


/**
  Get the waiting time given the wait's starting time.

  @param[in]  start_tv  the starting time

  @return
    @retval >= 0  the waiting time in microseconds(us)
    @retval  < 0  error in gettimeofday or time back traverse
*/

static int get_wait_time(const struct timeval * start_tv)
{
  ulonglong start_usecs, end_usecs;
  struct timeval end_tv;
  int end_time_err;

  /* Starting time in microseconds(us). */
  start_usecs= start_tv->tv_sec * TIME_MILLION + start_tv->tv_usec;

  /* Get the wait time interval. */
  end_time_err= gettimeofday(&end_tv, 0);

  /* Ending time in microseconds(us). */
  end_usecs= end_tv.tv_sec * TIME_MILLION + end_tv.tv_usec;

  if (end_time_err != 0 || end_usecs < start_usecs)
    return -1;

  return (int)(end_usecs - start_usecs);
}
