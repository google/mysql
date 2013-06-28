// Copyright (c) 2013 Google Inc. All Rights Reserved.

#ifndef SNIPER_MODULES_INCLUDED
#define SNIPER_MODULES_INCLUDED

#include "sql_class.h"
#include "sniper.h"

extern bool sniper_connectionless;
extern bool sniper_ignore_unauthenticated;
extern uint sniper_idle_timeout;
extern uint sniper_long_query_timeout;

extern double sniper_infeasible_max_cross_product_rows;
extern uint sniper_infeasible_max_time;

extern ulong sniper_infeasible_secondary_requirements;
typedef enum
{
  NONE= 0,
  FILESORT,
  TEMPORARY,
  FILESORT_AND_TEMPORARY,
  FILESORT_OR_TEMPORARY,
  ILLEGAL= 255
} SNIPER_INFEASIBLE_SECONDARY_REQUIREMENTS;

class Sniper_module_priv_ignore :public Sniper_module
{
private:
  ulong ignored;
public:
  Sniper_module_priv_ignore(ulong ignored_privs)
      :Sniper_module("Sniper_module_priv_ignore",
                    "Will return FALSE if the thread has any of the "
                    "permissions given."),
       ignored(ignored_privs) {};
  virtual bool should_snipe(THD *target_thd);

  void set_ignored_privs(ulong ignored_privs)
  {
    config_enter();
    ignored= ignored_privs;
    config_exit();
  }
};

class Sniper_module_idle :public Sniper_module
{
private:
  uint max_time;
public:
  Sniper_module_idle(uint timeout)
      :Sniper_module("Sniper_module_idle",
                    "Will return true if the thread has been idle "
                    "for more the given number of seconds."),
       max_time(timeout) {};
  virtual bool should_snipe(THD *target_thd);

  void set_timeout(uint timeout)
  {
    config_enter();
    max_time= timeout;
    config_exit();
  }
};

class Sniper_module_connectionless :public Sniper_module
{
public:
  Sniper_module_connectionless()
      :Sniper_module("Sniper_module_connectionless",
                    "Will return true if a user is not connected to "
                    "the server") {};
  virtual bool should_snipe(THD *target_thd);
};

class Sniper_module_unauthenticated :public Sniper_module
{
public:
  Sniper_module_unauthenticated()
      :Sniper_module("Sniper_module_unauthenticated",
                    "Will only return true if the user is "
                    "authenticated.") {};
  virtual bool should_snipe(THD *target_thd);
};

class Sniper_module_long_query :public Sniper_module
{
private:
  uint max_time;
public:
  Sniper_module_long_query(uint time)
      :Sniper_module("Sniper_module_long_query",
                    "Will return true if the command is something "
                    "other then \"sleep\" or \"binlog dump\" and it "
                    "has been running for longer then the given "
                    "time."),
      max_time(time) {};
  virtual bool should_snipe(THD *target_thd);

  void set_max_time(uint time)
  {
    config_enter();
    max_time= time;
    config_exit();
  }
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
                     "Will return true if the command is judged to be "
                     "infeasible. This decision is made based on the "
                     "cross-product of all the tables and whether the "
                     "query requires the use of filesort or temporary "
                     "tables. This may allow a THD to continue running "
                     "for a short time in order to give it the chance "
                     "to finish quickly."),
      max_cross_product_rows(max_cross_product_rows_arg),
      max_time(max_time_arg)
  {
    switch(secondary_requirements_arg)
    {
    case NONE:
      secondary_requirements= NONE;
      break;
    case FILESORT:
      secondary_requirements= FILESORT;
      break;
    case TEMPORARY:
      secondary_requirements= TEMPORARY;
      break;
    case FILESORT_AND_TEMPORARY:
      secondary_requirements= FILESORT_AND_TEMPORARY;
      break;
    case FILESORT_OR_TEMPORARY:
      secondary_requirements= FILESORT_OR_TEMPORARY;
      break;
    default:
      secondary_requirements= ILLEGAL;
      break;
    }
  };

  virtual bool should_snipe(THD *target_thd);
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
    case NONE:
      set_secondary_requirements(NONE);
      break;
    case FILESORT:
      set_secondary_requirements(FILESORT);
      break;
    case TEMPORARY:
      set_secondary_requirements(TEMPORARY);
      break;
    case FILESORT_AND_TEMPORARY:
      set_secondary_requirements(FILESORT_AND_TEMPORARY);
      break;
    case FILESORT_OR_TEMPORARY:
      set_secondary_requirements(FILESORT_OR_TEMPORARY);
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
};

extern Sniper sniper;
extern Sniper_module_idle sniper_module_idle;
extern Sniper_module_connectionless sniper_module_connectionless;
extern Sniper_module_unauthenticated sniper_module_unauthenticated;
extern Sniper_module_long_query sniper_module_long_query;
extern Sniper_module_priv_ignore sniper_module_priv_ignore;
extern Sniper_module_infeasible sniper_module_infeasible;

#endif  // SNIPER_MODULES_INCLUDED
