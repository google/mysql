// Copyright (c) 2013 Google Inc. All Rights Reserved.

#include "my_global.h"
#include <my_pthread.h>
#include "sql_class.h"
#include "sql_lex.h"
#include "sniper.h"
#include "sniper_modules.h"
#include "mysql_com.h"
#include "sql_acl.h"

Sniper sniper;
Sniper_module_idle sniper_module_idle(0);
Sniper_module_connectionless sniper_module_connectionless;
Sniper_module_unauthenticated sniper_module_unauthenticated;
Sniper_module_long_query sniper_module_long_query(0);
Sniper_module_priv_ignore sniper_module_priv_ignore(SUPER_ACL);
Sniper_module_infeasible sniper_module_infeasible(0.0,0,0);
Sniper_module_system_user_ignore sniper_module_system_user_ignore;

bool sniper_ignore_unauthenticated;
bool sniper_connectionless;

uint sniper_idle_timeout;
uint sniper_long_query_timeout;

bool sniper_infeasible_used;
double sniper_infeasible_max_cross_product_rows;
uint sniper_infeasible_max_time;
ulong sniper_infeasible_secondary_requirements;

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

bool Sniper_module_infeasible::should_snipe(THD *target_thd)
{
  QUERY_INFEASIBILITY infeasibility;
  if (!target_thd->lex || target_thd->get_command() != COM_QUERY)
    return FALSE;
  if (mysql_mutex_trylock(&(target_thd->LOCK_thd_data)))
    return FALSE;

  infeasibility= target_thd->lex->unit.get_infeasibility();
  mysql_mutex_unlock(&(target_thd->LOCK_thd_data));
  if (!(infeasibility.cross_product_rows >= max_cross_product_rows &&
      (max_time == 0 || (time(0) - target_thd->start_time) > max_time)))
    return FALSE;

  switch (secondary_requirements)
  {
  case NONE:
    return TRUE;
  case FILESORT:
    return infeasibility.uses_filesort;
  case TEMPORARY:
    return infeasibility.uses_temporary;
  case FILESORT_AND_TEMPORARY:
    return infeasibility.uses_filesort && infeasibility.uses_temporary;
  case FILESORT_OR_TEMPORARY:
    return infeasibility.uses_filesort || infeasibility.uses_temporary;
  default:// Should not happen
    return FALSE;
  }
}

bool Sniper_module_system_user_ignore::should_snipe(THD *target_thd)
{
  return !target_thd->security_ctx->is_system_user;
}
