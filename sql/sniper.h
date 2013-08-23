// Copyright (c) 2013 Google Inc. All Rights Reserved.
#ifndef SNIPER_INCLUDED
#define SNIPER_INCLUDED

#include <my_global.h>
#include <my_pthread.h>
#include <my_list.h>
#include "sql_class.h"
#include "sniper_structs.h"

extern bool sniper_active;
extern uint sniper_check_period;

typedef void * sniper_module_id;

typedef enum {
  MUST_NOT_SNIPE,
  NO_OPINION,
  MAY_SNIPE
} SNIPER_DECISION;

class Sniper_module
{
private:
  pthread_mutex_t LOCK_config;
protected:
  virtual SNIPER_DECISION decide(THD *target_thd, SNIPER_SETTINGS *settings)
  {
    return NO_OPINION;
  }
  ulong *kill_count_ptr;
public:
  const char *name;
  const char *description;
  Sniper_module(const char *module_name,
                const char *module_desc,
                ulong *kill_count)
      :kill_count_ptr(kill_count), name(module_name), description(module_desc)
  {
    pthread_mutex_init(&LOCK_config, NULL);
  }
  Sniper_module(const char *module_name, const char *module_desc)
      :kill_count_ptr(NULL), name(module_name), description(module_desc)
  {
    pthread_mutex_init(&LOCK_config, NULL);
  }
  virtual ~Sniper_module() {}
  virtual void shutdown() {}
  SNIPER_DECISION get_decision(THD *target_thd, SNIPER_SETTINGS *settings)
  {
    SNIPER_DECISION res;
    config_enter();
    res= decide(target_thd, settings);
    config_exit();
    return res;
  }
  virtual void increment_killed_count()
  {
    if (kill_count_ptr)
      (*kill_count_ptr)++;
  }
protected:
  void config_enter() { pthread_mutex_lock(&LOCK_config); }
  void config_exit()  { pthread_mutex_unlock(&LOCK_config); }
};

void *sniper_periodic_thread(void *);
class Sniper
{
private:
  pthread_t periodic_thread;
  uint period;
  bool periodic_running;
  bool running;
  LIST *periodic_checks;

  pthread_mutex_t LOCK_startup;
  pthread_mutex_t LOCK_register;
  pthread_mutex_t LOCK_periodic;
  pthread_cond_t  COND_periodic;
public:
  Sniper(uint interval= 0);
  virtual ~Sniper();
  void clean_up();
  void start();
  inline void stop()
  {
    sql_print_information("Sniper: stop called on Sniper");
    real_stop(TRUE);
  };
  inline bool is_started() { return running; };
  inline uint get_period() { return period; };
  void set_period(uint new_period);
  sniper_module_id register_module(Sniper_module *module);
  Sniper_module *unregister_module(sniper_module_id module);
private:
  void init();
  // This should only be used to stop the sniper from within the periodic
  // check thread.
  void stop_from_periodic_thread();
  void real_stop(bool should_lock);
  friend void *sniper_periodic_thread(void *);
  void start_periodic_thread();
  void stop_periodic_thread();
  void shoot(THD *target_thd);
  void do_sniping();
  bool should_shoot(THD *target_thd);
  virtual int do_wait();
};
#endif  // SNIPER_INCLUDED
