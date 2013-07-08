// Copyright (c) 2013 Google Inc. All Rights Reserved.

#include "my_global.h"
#include <my_pthread.h>
#include "sniper_modules.h"
#include "structs.h"
#include "sql_class.h"
#include "sql_lex.h"
#include "sniper.h"
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

double sniper_infeasible_max_cross_product_rows;
uint sniper_infeasible_max_time;
ulong sniper_infeasible_secondary_requirements;

SNIPER_DECISION Sniper_module_priv_ignore::decide(THD *target_thd,
                                                  SNIPER_SETTINGS *settings)
{
  Security_context *ctx = target_thd->security_ctx;
  if ((ctx->master_access | ctx->db_access) & ignored)
    return MUST_NOT_SNIPE;
  return NO_OPINION;
}

SNIPER_DECISION Sniper_module_idle::decide(THD *target_thd,
                                           SNIPER_SETTINGS *settings)
{
  uint timeout= settings->get_idle_timeout(max_time);
  if (timeout != 0 && target_thd->get_command() == COM_SLEEP &&
      (time(0) - target_thd->start_time) > timeout)
    return MAY_SNIPE;
  return NO_OPINION;
}

SNIPER_DECISION Sniper_module_unauthenticated::decide(THD *target_thd,
                                                      SNIPER_SETTINGS *settings)
{
  if (active && target_thd->security_ctx->user == NULL)
    return MUST_NOT_SNIPE;
  return NO_OPINION;
}

SNIPER_DECISION Sniper_module_connectionless::decide(THD *target_thd,
                                                     SNIPER_SETTINGS *settings)
{
  bool should_kill= settings->get_kill_connectionless(active);
  if (should_kill && !target_thd->is_connected())
    return MAY_SNIPE;
  return NO_OPINION;
}

SNIPER_DECISION Sniper_module_long_query::decide(THD *target_thd,
                                                 SNIPER_SETTINGS *settings)
{
  uint timeout= settings->get_long_query_timeout(max_time);
  enum enum_server_command cmd= target_thd->get_command();
  if (timeout != 0 && cmd != COM_SLEEP &&
      (time(0) - target_thd->start_time) > timeout)
    return MAY_SNIPE;
  return NO_OPINION;
}

SNIPER_DECISION Sniper_module_infeasible::decide(THD *target_thd,
                                                 SNIPER_SETTINGS *settings)
{
  double cp_rows= settings->get_infeasible_cross_product_rows(
                    max_cross_product_rows);
  uint timeout=  settings->get_infeasible_max_time(max_time);
  SNIPER_INFEASIBLE_SECONDARY_REQUIREMENTS reqs=
      settings->get_infeasible_secondary_reqs(secondary_requirements);

  QUERY_INFEASIBILITY infeasibility;
  if (!cp_rows || !target_thd->lex || target_thd->get_command() != COM_QUERY)
    return NO_OPINION;
  if (mysql_mutex_trylock(&(target_thd->LOCK_thd_data)))
    return NO_OPINION;

  infeasibility= target_thd->lex->unit.get_infeasibility();
  mysql_mutex_unlock(&(target_thd->LOCK_thd_data));
  if (!(infeasibility.cross_product_rows >= cp_rows &&
        (timeout == 0 || (time(0) - target_thd->start_time) > timeout)))
    return NO_OPINION;

  bool ret;
  switch (reqs)
  {
  case REQUIRES_NONE:
    ret= TRUE;
    break;
  case REQUIRES_FILESORT:
    ret= infeasibility.uses_filesort;
    break;
  case REQUIRES_TEMPORARY:
    ret= infeasibility.uses_temporary;
    break;
  case REQUIRES_FILESORT_AND_TEMPORARY:
    ret= infeasibility.uses_filesort && infeasibility.uses_temporary;
    break;
  case REQUIRES_FILESORT_OR_TEMPORARY:
    ret= infeasibility.uses_filesort || infeasibility.uses_temporary;
    break;
  default:// Should not happen
    ret= FALSE;
    break;
  }
  return (ret) ? MAY_SNIPE : NO_OPINION;
}

SNIPER_DECISION Sniper_module_system_user_ignore::decide(THD *target_thd,
                                                         SNIPER_SETTINGS *settings)
{
  if (target_thd->security_ctx->is_system_user)
    return MUST_NOT_SNIPE;
  return NO_OPINION;
}
