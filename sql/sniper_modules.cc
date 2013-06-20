// Copyright (c) 2013 Google Inc. All Rights Reserved.

#include "my_global.h"
#include "sql_class.h"
#include "sniper.h"
#include "sniper_modules.h"
#include "mysql_com.h"

bool sniper_ignore_unauthenticated;
bool sniper_connectionless;

uint sniper_idle_timeout;
uint sniper_long_query_timeout;

bool Sniper_module_priv_ignore::should_snipe(THD *target_thd)
{
  Security_context *ctx = target_thd->security_ctx;
  return ((ctx->master_access | ctx->db_access) & ignored) == 0;
}

bool Sniper_module_idle::should_snipe(THD *target_thd)
{
  return target_thd->get_command() == COM_SLEEP &&
      (time(0) - target_thd->start_time) > max_time;
}

bool Sniper_module_unauthenticated::should_snipe(THD *target_thd)
{
  return target_thd->security_ctx->user != NULL;
}

bool Sniper_module_connectionless::should_snipe(THD *target_thd)
{
  return !target_thd->is_connected();
}

bool Sniper_module_long_query::should_snipe(THD *target_thd)
{
  enum enum_server_command cmd= target_thd->get_command();
  return cmd != COM_SLEEP && cmd != COM_BINLOG_DUMP &&
      (time(0) - target_thd->start_time) > max_time;
}

