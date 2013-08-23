// Copyright (c) 2013 Google Inc. All Rights Reserved.

#include <my_global.h>
#include <my_pthread.h>
#include "thr_alarm.h"
#include "sql_class.h"
#include "sql_callback.h"
#include "sniper.h"
#include "sniper_modules.h"
#include "sql_acl.h"
#include <my_list.h>

struct sniper_action_arg
{
  THD *target_thd;
  SNIPER_SETTINGS *settings;
};

bool sniper_active;
uint sniper_check_period;

Sniper::Sniper(uint interval)
  : period(interval),
    periodic_running(FALSE),
    running(FALSE),
    periodic_checks(NULL)
{
  DBUG_ASSERT(this == &sniper);
  init();
}

void Sniper::init()
{
  pthread_mutex_init(&LOCK_startup, NULL);
  pthread_mutex_init(&LOCK_register, NULL);
  pthread_mutex_init(&LOCK_periodic, NULL);
  pthread_cond_init(&COND_periodic, NULL);
}

Sniper::~Sniper()
{
  clean_up();
}

static int call_shutdown(void *mod, void *nothing)
{
  ((Sniper_module*)mod)->shutdown();
  return FALSE;
}

void Sniper::clean_up()
{
  pthread_mutex_lock(&LOCK_startup);
  real_stop(FALSE);
  pthread_mutex_lock(&LOCK_register);
  list_walk(periodic_checks, (list_walk_action)(&call_shutdown),NULL);
  list_free(periodic_checks, FALSE);
  periodic_checks= NULL;
  pthread_mutex_unlock(&LOCK_register);
  pthread_mutex_unlock(&LOCK_startup);
}

void Sniper::start()
{
  sql_print_information("Sniper: start called on Sniper");
  bool pstart= FALSE;
  pthread_mutex_lock(&LOCK_startup);
  if (running)
    goto end;
  running= TRUE;
  pthread_mutex_lock(&LOCK_register);
  pstart= periodic_checks != NULL;
  pthread_mutex_unlock(&LOCK_register);
  if (pstart)
  {
    pthread_mutex_lock(&LOCK_periodic);
    start_periodic_thread();
    pthread_mutex_unlock(&LOCK_periodic);
  }
end:
  pthread_mutex_unlock(&LOCK_startup);
}

void Sniper::real_stop(bool should_lock)
{
  if (should_lock)
    pthread_mutex_lock(&LOCK_startup);
  if (running)
  {
    running= FALSE;
    pthread_mutex_lock(&LOCK_periodic);
    if (periodic_running)
    {
      pthread_t thrd= periodic_thread;
      stop_periodic_thread();
      pthread_mutex_unlock(&LOCK_periodic);
      pthread_join(thrd, NULL);
    }
    else
      pthread_mutex_unlock(&LOCK_periodic);
  }
  if (should_lock)
    pthread_mutex_unlock(&LOCK_startup);
}

void Sniper::set_period(uint new_period)
{
  pthread_mutex_lock(&LOCK_periodic);
  sql_print_information("Sniper: check period set to %i",new_period);
  if (new_period == 0)
  {
    if (period == 0)
      goto end;
    period= 0;
    if (periodic_running)
    {
      stop_periodic_thread();
      pthread_mutex_unlock(&LOCK_periodic);
      pthread_join(periodic_thread, NULL);
    }
    else
      pthread_mutex_unlock(&LOCK_periodic);
    return;
  }
  else if (period == 0)
  {
    period= new_period;
    start_periodic_thread();
  }
  else
  {
    period= new_period;
    pthread_cond_broadcast(&COND_periodic);
  }
end:
  pthread_mutex_unlock(&LOCK_periodic);
  return;
}

static int list_contains_callback(void *data, void *want)
{
  return data==want;
}

inline static int list_contains(LIST *list, void* data)
{
  return list_walk(list,(list_walk_action)(&list_contains_callback),
                   (uchar*)data);
}

sniper_module_id Sniper::register_module(Sniper_module *module)
{
  pthread_mutex_lock(&LOCK_register);
  if (!list_contains(periodic_checks, module))
  {
    list_push(periodic_checks, module);
    sql_print_information("Sniper: %s (%s) registered to the Sniper.",
                          module->name, module->description);
  }
  if (periodic_checks != NULL)
  {
    pthread_mutex_unlock(&LOCK_register);
    pthread_mutex_lock(&LOCK_periodic);
    start_periodic_thread();
    pthread_mutex_unlock(&LOCK_periodic);
  }
  else
    pthread_mutex_unlock(&LOCK_register);
  return (sniper_module_id)module;
}

Sniper_module *Sniper::unregister_module(sniper_module_id module)
{
  Sniper_module *ret= NULL;
  LIST *root= periodic_checks;
  while (root != NULL)
  {
    if ((Sniper_module*)root->data == (Sniper_module*)module)
    {
      ret= (Sniper_module*)module;
      LIST *cur= root;
      root= root->next;
      periodic_checks= list_delete(periodic_checks, cur);
      my_free(cur);
    }
    else
    {
      root= root->next;
    }
  }
  if (ret)
  {
    sql_print_information("Sniper: module %s (%s) unregistered from Sniper.",
                          ret->name, ret->description);
  }
  return ret;
}

void Sniper::stop_from_periodic_thread()
{
  DBUG_ASSERT(periodic_thread == pthread_self());
  sniper_active= FALSE;
  // Prevent a race with start.
  pthread_mutex_lock(&LOCK_startup);

  // Prevent a race where new thread created before this one gets detached.
  pthread_mutex_lock(&LOCK_periodic);
  stop_periodic_thread();
  pthread_detach(pthread_self());
  pthread_mutex_unlock(&LOCK_periodic);

  real_stop(FALSE);
  pthread_mutex_unlock(&LOCK_startup);
}

void *sniper_periodic_thread(void * arg)
{
  Sniper *snp = (Sniper*)arg;
  if (my_thread_init())
  {
    snp->periodic_running= FALSE;
    sql_print_error("Sniper: Could not initialize sniper thread");
    snp->stop_from_periodic_thread();
    return NULL;
  }
  int res;

  while (!abort_loop)
  {
    pthread_mutex_lock(&snp->LOCK_periodic);
    if (abort_loop || !snp->periodic_running || !snp->running
        || snp->period == 0)
    {
      pthread_mutex_unlock(&snp->LOCK_periodic);
      break;
    }
    res= snp->do_wait();
    if (abort_loop || !snp->periodic_running || !snp->running)
    {
      pthread_mutex_unlock(&snp->LOCK_periodic);
      break;
    }
    pthread_mutex_unlock(&snp->LOCK_periodic);
    if (res == ETIMEDOUT)
      snp->do_sniping();
    else if (res == 0)
      continue;
    else
    {
      sql_print_error("Sniper: Error %i received on timedwait", res);
      snp->stop_from_periodic_thread();
      break;
    }
  }
  my_thread_end();
  return NULL;
}

int Sniper::do_wait()
{
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += period;
  return pthread_cond_timedwait(&COND_periodic, &LOCK_periodic, &ts);
}

// must be called with LOCK_periodic
void Sniper::start_periodic_thread()
{
  if (!running || periodic_running || period == 0)
    return;
  periodic_running= TRUE;

  pthread_attr_t attrs;
  pthread_attr_init(&attrs);
  pthread_attr_setscope(&attrs, PTHREAD_SCOPE_SYSTEM);
  pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&periodic_thread, &attrs,
                     sniper_periodic_thread, (this)))
  {
    sql_print_error("Sniper: Cannot create sniper periodic thread.");
    periodic_running= FALSE;
    running= FALSE;
    sniper_active= FALSE;
  }
}

// must be called with LOCK_periodic
void Sniper::stop_periodic_thread()
{
  periodic_running= FALSE;
  pthread_cond_broadcast(&COND_periodic);
}

void Sniper::shoot(THD *target_thd)
{
  mysql_mutex_lock(&(target_thd->LOCK_thd_data));
  target_thd->awake_timed(KILL_CONNECTION, 0, 0);
  mysql_mutex_unlock(&(target_thd->LOCK_thd_data));
}

// Print out all the modules that approved of a sniping.
static int walk_action_print_and_incr_approvals(void *module, void *thread)
{
  Sniper_module *mod= (Sniper_module *)module;
  THD *target_thd= (THD *)thread;
  mod->increment_killed_count();
  sql_print_information("Sniper: THD id=%lu approved for sniping by "
                        "%s (%s).", target_thd->thread_id, mod->name,
                        mod->description);
  return 0;
}

bool Sniper::should_shoot(THD *target_thd)
{
  // We have already obtained a lock on LOCK_sniper_config in the do_sniping
  // function. We do that there to prevent a possible deadlock in sql_acl.cc
  SNIPER_SETTINGS settings;
  Security_context *ctx= target_thd->security_ctx;
  mysql_mutex_assert_owner(&LOCK_sniper_config);
  sniper_settings_get(&settings, ctx, TRUE);

  bool ret= FALSE; // If nothing cares we do not snipe.
  LIST *approvals= NULL; // Hold onto modules that approve for logging.

  for (LIST *root= periodic_checks; root; root= root->next)
  {
    Sniper_module *mod= (Sniper_module *)root->data;
    switch (mod->get_decision(target_thd, &settings))
    {
    case MUST_NOT_SNIPE:
      // The sniping has been rejected. We can stop looking through the
      // modules right now.
      ret= FALSE;
      goto end;
      break;
    case MAY_SNIPE:
      // Save the module so we can credit it with the kill later, if that is,
      // we are not rejected.
      list_push(approvals, mod);
      ret= TRUE;
      break;
    case NO_OPINION:
      // This module doesn't care. Continue on.
      break;
    }
  }
  // If we are here that means that no module flat out rejected the sniping.
  // Therefore we might have decided to kill a thread and should log it if we
  // have.
  if (ret)
  {
    // We got no absolute rejections and some provisional approvals. We should
    // print out all modules which approved the sniping.
    list_walk(approvals,
              (list_walk_action)walk_action_print_and_incr_approvals,
              (unsigned char *)target_thd);
  }
end:
  // Clean up the list of approvals. Don't destroy the modules though.
  list_free(approvals, FALSE);
  return ret;
}

void Sniper::do_sniping()
{
  sniper_runs++;
  // Locking this for use in should_shoot. We do this here to prevent
  // a possible deadlock when acl_kill_user_threads locks LOCK_thread_count
  // after locking LOCK_sniper_config when running a DROP USER command.
  mysql_mutex_lock(&LOCK_sniper_config);
  mysql_mutex_lock(&LOCK_thread_count);
  I_List_iterator<THD> it(threads);
  THD *target_thd;
  if (threads.is_empty())
    goto end;
  pthread_mutex_lock(&LOCK_register);
  while ((target_thd= it++))
  {
    // We probably do not want to be killing system threads for any reason.
    if (target_thd->system_thread == NON_SYSTEM_THREAD &&
        should_shoot(target_thd))
    {
      Security_context *ctx= target_thd->security_ctx;
      sql_print_information("Sniper: Attempting to kill THD id=%lu, "
                            "run by '%s@%s'",
                            target_thd->thread_id,
                            (ctx && ctx->user) ? ctx->user : "",
                            (ctx && ctx->host) ? ctx->host : "");
      sniper_queries_killed++;
      shoot(target_thd);
    }
  }
  pthread_mutex_unlock(&LOCK_register);
end:
  mysql_mutex_unlock(&LOCK_thread_count);
  mysql_mutex_unlock(&LOCK_sniper_config);
}

