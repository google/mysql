// Copyright (c) 2013 Google Inc. All Rights Reserved.

#include "my_global.h"
#include "sql_class.h"
#include "sniper.h"
#include "sniper_modules.h"
#include "mysql_com.h"

bool sniper_ignore_unauthenticated;

uint sniper_idle_timeout;

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
