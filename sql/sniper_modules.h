// Copyright (c) 2013 Google Inc. All Rights Reserved.

#ifndef SNIPER_MODULES_INCLUDED
#define SNIPER_MODULES_INCLUDED

#include "sql_class.h"
#include "sniper_structs.h"
#include "sniper.h"

extern bool sniper_connectionless;
extern bool sniper_ignore_unauthenticated;
extern uint sniper_idle_timeout;
extern uint sniper_long_query_timeout;

extern double sniper_infeasible_max_cross_product_rows;
extern uint sniper_infeasible_max_time;

extern ulong sniper_infeasible_secondary_requirements;

class Sniper_module_priv_ignore :public Sniper_module
{
private:
  ulong ignored;
public:
  Sniper_module_priv_ignore(ulong ignored_privs)
      :Sniper_module("Sniper_module_priv_ignore",
                     "Will VETO sniping if the thread has any of the "
                     "permissions given."),
       ignored(ignored_privs) {};

  void set_ignored_privs(ulong ignored_privs)
  {
    config_enter();
    ignored= ignored_privs;
    config_exit();
  }
protected:
  virtual SNIPER_DECISION decide(THD *target_thd, SNIPER_SETTINGS *settings);
};

class Sniper_module_idle :public Sniper_module
{
private:
  uint max_time;
public:
  Sniper_module_idle(uint timeout)
      :Sniper_module("Sniper_module_idle",
                     "Will APPROVE sniping if the thread has been idle "
                     "for more the given number of seconds.",
                    &sniper_idle_queries_killed),
       max_time(timeout) {};

  void set_timeout(uint timeout)
  {
    config_enter();
    max_time= timeout;
    config_exit();
  }
protected:
  virtual SNIPER_DECISION decide(THD *target_thd, SNIPER_SETTINGS *settings);
};

class Sniper_module_connectionless :public Sniper_module
{
private:
  bool active;
public:
  Sniper_module_connectionless()
      :Sniper_module("Sniper_module_connectionless",
                     "Will APPROVE sniping if a user is not connected to "
                     "the server", &sniper_connectionless_queries_killed),
       active(TRUE) {};
  void set_active(bool is_active)
  {
    config_enter();
    active= is_active;
    config_exit();
  }
protected:
  virtual SNIPER_DECISION decide(THD *target_thd, SNIPER_SETTINGS *settings);
};

class Sniper_module_unauthenticated :public Sniper_module
{
private:
  bool active;
public:
  Sniper_module_unauthenticated()
      :Sniper_module("Sniper_module_unauthenticated",
                     "Will VETO sniping if the user is not"
                     "authenticated yet."),
       active(TRUE) {};
  void set_active(bool is_active)
  {
    config_enter();
    active= is_active;
    config_exit();
  }
protected:
  virtual SNIPER_DECISION decide(THD *target_thd, SNIPER_SETTINGS *settings);
};

class Sniper_module_long_query :public Sniper_module
{
private:
  uint max_time;
public:
  Sniper_module_long_query(uint time)
      :Sniper_module("Sniper_module_long_query",
                     "Will APPROVE sniping if the command is something "
                     "other then \"sleep\" and it has been running for "
                     "longer then the given time.",
                     &sniper_long_queries_killed),
      max_time(time) {};

  void set_max_time(uint time)
  {
    config_enter();
    max_time= time;
    config_exit();
  }
protected:
  virtual SNIPER_DECISION decide(THD *target_thd, SNIPER_SETTINGS *settings);
};

class Sniper_module_infeasible : public Sniper_module
{
private:
  double max_cross_product_rows;
  uint max_time;
  SNIPER_INFEASIBLE_SECONDARY_REQUIREMENTS secondary_requirements;
public:
  Sniper_module_infeasible(double max_cross_product_rows_arg,
                           uint max_time_arg,
                           ulong secondary_requirements_arg)
      :Sniper_module("Sniper_module_infeasible",
                     "Will APPROVE sniping if the command is judged to be "
                     "infeasible. This decision is made based on the "
                     "cross-product of all the tables and whether the "
                     "query requires the use of filesort or temporary "
                     "tables. This may allow a THD to continue running "
                     "for a short time in order to give it the chance "
                     "to finish quickly.",
                     &sniper_infeasible_queries_killed),
      max_cross_product_rows(max_cross_product_rows_arg),
      max_time(max_time_arg)
  {
    set_secondary_requirements(secondary_requirements_arg);
  };

  void set_max_cross_product_rows(double new_max_cross_product)
  {
    config_enter();
    max_cross_product_rows= new_max_cross_product;
    config_exit();
  }
  void set_max_time(uint time)
  {
    config_enter();
    max_time= time;
    config_exit();
  }
  void set_secondary_requirements(ulong requirements)
  {
    switch(requirements)
    {
    case REQUIRES_NONE:
      set_secondary_requirements(REQUIRES_NONE);
      break;
    case REQUIRES_FILESORT:
      set_secondary_requirements(REQUIRES_FILESORT);
      break;
    case REQUIRES_TEMPORARY:
      set_secondary_requirements(REQUIRES_TEMPORARY);
      break;
    case REQUIRES_FILESORT_AND_TEMPORARY:
      set_secondary_requirements(REQUIRES_FILESORT_AND_TEMPORARY);
      break;
    case REQUIRES_FILESORT_OR_TEMPORARY:
      set_secondary_requirements(REQUIRES_FILESORT_OR_TEMPORARY);
      break;
    default:
      set_secondary_requirements(ILLEGAL);
      break;
    }
  }

  void set_secondary_requirements(
      SNIPER_INFEASIBLE_SECONDARY_REQUIREMENTS requirements)
  {
    config_enter();
    secondary_requirements= requirements;
    config_exit();
  }
protected:
  virtual SNIPER_DECISION decide(THD *target_thd, SNIPER_SETTINGS *settings);
};

class Sniper_module_system_user_ignore : public Sniper_module
{
public:
  Sniper_module_system_user_ignore()
      : Sniper_module("Sniper_module_system_user_ignore",
                      "Will VETO sniping if the user is a system_user "
                      "so it can be ignored") {}
protected:
  virtual SNIPER_DECISION decide(THD *target_thd, SNIPER_SETTINGS *settings);
};

extern Sniper sniper;
extern Sniper_module_idle sniper_module_idle;
extern Sniper_module_connectionless sniper_module_connectionless;
extern Sniper_module_unauthenticated sniper_module_unauthenticated;
extern Sniper_module_long_query sniper_module_long_query;
extern Sniper_module_priv_ignore sniper_module_priv_ignore;
extern Sniper_module_infeasible sniper_module_infeasible;
extern Sniper_module_system_user_ignore sniper_module_system_user_ignore;

#endif  // SNIPER_MODULES_INCLUDED
