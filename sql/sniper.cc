// Copyright (c) 2013 Google Inc. All Rights Reserved.

#include <my_global.h>
#include <my_pthread.h>
#include "thr_alarm.h"
#include "sql_class.h"
#include "sql_callback.h"
#include "sniper.h"
#include <my_list.h>

bool sniper_active;
uint sniper_check_period;

Sniper::Sniper(uint interval)
  : period(interval),
    periodic_running(FALSE),
    running(FALSE),
    global_checks(NULL),
    periodic_checks(NULL)
{
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
  pthread_mutex_lock(&LOCK_startup);
  real_stop(FALSE);
  pthread_mutex_lock(&LOCK_register);
  delete_list(global_checks);
  delete_list(periodic_checks);
  pthread_mutex_unlock(&LOCK_register);
  pthread_mutex_unlock(&LOCK_startup);
}

// Needed because the list_free method assumes that the data was allocated
// using my_malloc, which this wasn't as it was made by using new.
void Sniper::delete_list(LIST *lst)
{
  LIST *next;
  while (lst)
  {
    delete (Sniper_module*)(lst->data);
    next= lst->next;
    my_free(lst);
    lst= next;
  }
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
      stop_periodic_thread();
      pthread_mutex_unlock(&LOCK_periodic);
      pthread_join(periodic_thread, NULL);
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

sniper_module_id Sniper::register_global_check(Sniper_module *module)
{
  sql_print_information("Sniper: %s (%s) registered as a global_check to Sniper.",
                        module->name, module->description);
  pthread_mutex_lock(&LOCK_register);
  list_push(global_checks, module);
  pthread_mutex_unlock(&LOCK_register);
  return (sniper_module_id)module;
}

sniper_module_id Sniper::register_periodic_check(Sniper_module *module)
{
  sql_print_information("Sniper: %s (%s) registered as a periodic_check to Sniper.",
                        module->name, module->description);
  pthread_mutex_lock(&LOCK_register);
  list_push(periodic_checks, module);
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

Sniper_module *Sniper::unregister_global_check(sniper_module_id module)
{
  Sniper_module *ret= NULL;
  LIST *root= global_checks;
  while (root != NULL)
  {
    if ((Sniper_module*)root->data == (Sniper_module*)module)
    {
      ret= (Sniper_module*)module;
      LIST *cur= root;
      root= root->next;
      global_checks= list_delete(global_checks, cur);
      my_free(cur);
    }
    else
    {
      root= root->next;
    }
  }
  if (ret)
  {
    sql_print_information("Sniper: global_check %s (%s) unregistered from Sniper.",
                          ret->name, ret->description);
  }
  return ret;
}

Sniper_module *Sniper::unregister_periodic_check(sniper_module_id module)
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
    sql_print_information("Sniper: periodic_check %s (%s) unregistered from Sniper.",
                          ret->name, ret->description);
  }
  return ret;
}

void *sniper_periodic_thread(void * arg)
{
  Sniper *snp = (Sniper*)arg;
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
      break;
    }
  }
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
    sql_print_warning("Sniper: Cannot create sniper periodic thread.");
    periodic_running= FALSE;
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

static int walk_action_global(void *data, void *arg)
{
  return !((Sniper_module*)data)->should_snipe((THD*)arg);
}

static int walk_action_periodic(void *data, void *arg)
{
  THD *target_thd= (THD*)arg;
  Sniper_module *mod= (Sniper_module*)data;
  if (mod->should_snipe(target_thd))
  {
    sql_print_information("Sniper: THD id=%lu approved for sniping by "
                          "%s (%s).", target_thd->thread_id, mod->name,
                          mod->description);
    return 1;
  }
  else
    return 0;
}

bool Sniper::should_shoot(THD *target_thd)
{
  return !list_walk(global_checks, (list_walk_action)(&walk_action_global),
                    (uchar*)target_thd) &&
         list_sum(periodic_checks, (list_walk_action)(&walk_action_periodic),
                  (uchar *)target_thd);
}

void Sniper::do_sniping()
{
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

      shoot(target_thd);
    }
  }
  pthread_mutex_unlock(&LOCK_register);
end:
  mysql_mutex_unlock(&LOCK_thread_count);
}

