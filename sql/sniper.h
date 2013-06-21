// Copyright (c) 2013 Google Inc. All Rights Reserved.
#ifndef SNIPER_INCLUDED
#define SNIPER_INCLUDED

#include <my_global.h>
#include <my_pthread.h>
#include <my_list.h>
#include "sql_class.h"

extern bool sniper_active;
extern uint sniper_check_period;

typedef void * sniper_module_id;

class Sniper_module
{
private:
  pthread_mutex_t LOCK_config;
public:
  const char *name;
  const char *description;
  Sniper_module(const char *module_name, const char *module_desc)
  {
    name= module_name;
    description= module_desc;
    pthread_mutex_init(&LOCK_config, NULL);
  };
  virtual ~Sniper_module() {}
  virtual void shutdown() {return;}
  virtual bool should_snipe(THD *target_thd) {return FALSE;}
  bool does_approve(THD *target_thd)
  {
    bool res;
    config_enter();
    res= should_snipe(target_thd);
    config_exit();
    return res;
  }
protected:
  void config_enter() {pthread_mutex_lock  (&LOCK_config);}
  void config_exit()  {pthread_mutex_unlock(&LOCK_config);}
};

void *sniper_periodic_thread(void *);
class Sniper
{
private:
  pthread_t periodic_thread;
  uint period;
  bool periodic_running;
  bool running;
  LIST *global_checks;
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
  inline void stop() {
    sql_print_information("Sniper: stop called on Sniper");
    real_stop(TRUE);
  };
  inline bool is_started() {return running;};
  inline uint get_period() {return period;};
  void set_period(uint new_period);
  sniper_module_id register_global_check(Sniper_module *module);
  sniper_module_id register_periodic_check(Sniper_module *module);
  Sniper_module *unregister_global_check(sniper_module_id module);
  Sniper_module *unregister_periodic_check(sniper_module_id module);
private:
  void init();
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
