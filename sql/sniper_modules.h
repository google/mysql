// Copyright (c) 2013 Google Inc. All Rights Reserved.

#ifndef SNIPER_MODULES_INCLUDED
#define SNIPER_MODULES_INCLUDED

#include "sql_class.h"
#include "sniper.h"


extern bool sniper_connectionless;
extern bool sniper_ignore_unauthenticated;
extern uint sniper_idle_timeout;
extern uint sniper_long_query_timeout;

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
};

#endif  // SNIPER_MODULES_INCLUDED
