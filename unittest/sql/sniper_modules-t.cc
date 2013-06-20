// Copyright (c) 2013 Google Inc. All Rights Reserved.

/*
  This file does testing on the various included sniper modules.
*/

#define MYSQL_SERVER
#include <my_global.h>
#include <my_pthread.h>
#include <time.h>
/*
  What follows this is an extreemly ugly hack. Here I needed to be able to
  test whether the Sniper_module subclasses I had made worked, which required
  passing them in THD*'s in various different states. Unfortunately in order
  to allocate a THD object multiple pieces of mysql must already be setup,
  including the threading system and parts of the thread-specific memory
  allocation system. This means that in order to run these unit tests using
  the normal THD objects defined in sql_class.h I would need to essentially
  bring up an entire mysql server. In order to avoid that I used macro's to
  rewrite the names of the THD object in the rest of the program and then
  redefined my own which has only the data that I want to look at.

  This strategy is the same as is used in the my_apc-t test.
*/
#define THD THD_REAL
#define Security_context Security_context_REAL
#include "sql_class.h"
#include "sql_acl.h"
#undef THD
#undef Security_context

#include <tap.h>

class Security_context
{
public:
  ulong master_access;
  ulong db_access;
  char *user;
};
class THD
{
public:
  bool connected;
  bool is_connected() {return connected;};
  enum enum_server_command cmd;
  Security_context *security_ctx;
  enum enum_thread_type system_thread;
  my_time_t start_time;
  enum enum_server_command get_command() {return cmd;};
};

#include "sniper.h"
#include "sniper_modules.h"
#include "sniper_modules.cc"

// End of terrible things.

THD *thd;
Security_context *ctx;

#define IDLE_TEST_COUNT 4
void test_idle()
{
  Sniper_module_idle idle(20);
  thd->cmd=COM_SLEEP;
  thd->start_time= time(0)-5;
  ok(!idle.should_snipe(thd), "Under timeout.");

  thd->start_time= time(0)-30;
  ok(idle.should_snipe(thd), "Over timeout.");

  thd->cmd=COM_QUERY;
  thd->start_time= time(0)-5;
  ok(!idle.should_snipe(thd), "Under timeout without sleep.");

  thd->start_time= time(0)-30;
  ok(!idle.should_snipe(thd), "Over timeout without sleep.");
}

#define PRIV_TEST_COUNT 8
void test_priv()
{
  Sniper_module_priv_ignore priv(SUPER_ACL);

  ctx->master_access= DROP_ACL|CREATE_ACL;
  ctx->db_access= RELOAD_ACL|UPDATE_ACL;
  ok(priv.should_snipe(thd), "Catch bad ACL");

  ctx->master_access= DROP_ACL|CREATE_ACL|SUPER_ACL;
  ok(!priv.should_snipe(thd), "Catch good master ACL");

  ctx->master_access= DROP_ACL|CREATE_ACL;
  ctx->db_access= RELOAD_ACL|UPDATE_ACL|SUPER_ACL;
  ok(!priv.should_snipe(thd), "Catch good db ACL");

  ctx->master_access= DROP_ACL|CREATE_ACL|SUPER_ACL;
  ok(!priv.should_snipe(thd), "Catch good ACL on both");

  Sniper_module_priv_ignore priv2(SUPER_ACL|DROP_ACL);
  ok(!priv2.should_snipe(thd), "Catch good ACL on both with multi");

  ctx->master_access= INSERT_ACL|SUPER_ACL;
  ctx->db_access= RELOAD_ACL|DELETE_ACL;
  ok(!priv2.should_snipe(thd), "Catch good ACL on master with multi");

  ctx->master_access= RELOAD_ACL|DELETE_ACL;
  ctx->db_access= INSERT_ACL|DROP_ACL;
  ok(!priv2.should_snipe(thd), "Catch good ACL on db with multi");

  ctx->master_access= DELETE_ACL|CREATE_ACL;
  ctx->db_access= RELOAD_ACL|UPDATE_ACL;
  ok(priv2.should_snipe(thd), "Catch bad ACL with multi");
}

#define UNAUTHENTICATED_TEST_COUNT 2
void test_unauthenticated()
{
  Sniper_module_unauthenticated unauth;

  thd->system_thread= NON_SYSTEM_THREAD;
  ctx->user= NULL;
  ok(!unauth.should_snipe(thd), "Detect unauthenticated");

  ctx->user=(char*)"somebody";
  ok(unauth.should_snipe(thd), "Detect authenticated user");
}

int main(int argc, char **argv)
{
  thd= new THD;
  ctx= new Security_context;
  thd->security_ctx= ctx;
  plan(UNAUTHENTICATED_TEST_COUNT + PRIV_TEST_COUNT + IDLE_TEST_COUNT);
  test_unauthenticated();
  test_priv();
  test_idle();
  delete thd;
  delete ctx;
}
