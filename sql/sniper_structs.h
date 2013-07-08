// Copyright (c) 2013 Google Inc. All Rights Reserved.
#ifndef SNIPER_STRUCTS_INCLUDED
#define SNIPER_STRUCTS_INCLUDED

typedef enum sniper_infeasible_secondary_requirements
{
  REQUIRES_NONE= 0,
  REQUIRES_FILESORT,
  REQUIRES_TEMPORARY,
  REQUIRES_FILESORT_AND_TEMPORARY,
  REQUIRES_FILESORT_OR_TEMPORARY,
  ILLEGAL= 255
} SNIPER_INFEASIBLE_SECONDARY_REQUIREMENTS;

typedef struct sniper_settings
{
  /*
     Bitmask values of this enum and specified_settings member are used by the
     parser to store which user limits were specified in GRANT statement.
  */
  enum {
    IDLE_TIMEOUT= 1,
    LONG_QUERY_TIMEOUT= 2,
    KILL_CONNECTIONLESS= 4,
    INFEASIBLE_CROSS_PRODUCT_ROWS= 8,
    INFEASIBLE_MAX_TIME= 16,
    INFEASIBLE_SECONDARY_REQS= 32
  };

  /* Time the connection should be allowed to stay idle */
  uint idle_timeout;

  inline uint get_idle_timeout(uint default_val)
  {
    return (has_setting(IDLE_TIMEOUT)) ? idle_timeout : default_val;
  }

  inline void update_idle_timeout(struct sniper_settings *other)
  {
    if (other->has_setting(IDLE_TIMEOUT))
    {
      add_setting(IDLE_TIMEOUT);
      idle_timeout= other->idle_timeout;
    }
    else if (other->is_nulled(IDLE_TIMEOUT))
      remove_setting(IDLE_TIMEOUT);
  }

  /* Time after which a query should be killed. */
  uint long_query_timeout;

  inline uint get_long_query_timeout(uint default_val)
  {
    return (has_setting(LONG_QUERY_TIMEOUT)) ? long_query_timeout : default_val;
  }

  inline void update_long_query_timeout(struct sniper_settings *other)
  {
    if (other->has_setting(LONG_QUERY_TIMEOUT))
    {
      add_setting(LONG_QUERY_TIMEOUT);
      long_query_timeout= other->long_query_timeout;
    }
    else if (other->is_nulled(LONG_QUERY_TIMEOUT))
      remove_setting(LONG_QUERY_TIMEOUT);
  }

  /* True to allow a connectionless query to continue */
  my_bool kill_connectionless;

  inline my_bool get_kill_connectionless(my_bool default_val)
  {
    return (has_setting(KILL_CONNECTIONLESS)) ? kill_connectionless : default_val;
  }

  inline void update_kill_connectionless(struct sniper_settings *other)
  {
    if (other->has_setting(KILL_CONNECTIONLESS))
    {
      add_setting(KILL_CONNECTIONLESS);
      kill_connectionless= other->kill_connectionless;
    }
    else if (other->is_nulled(KILL_CONNECTIONLESS))
      remove_setting(KILL_CONNECTIONLESS);
  }

  /* Cross product which marks a query as possibly infeasible */
  double infeasible_cross_product_rows;

  inline double get_infeasible_cross_product_rows(double default_val)
  {
    return (has_setting(INFEASIBLE_CROSS_PRODUCT_ROWS)) ?
        infeasible_cross_product_rows : default_val;
  }

  inline void update_infeasible_cross_product_rows(struct sniper_settings *other)
  {
    if (other->has_setting(INFEASIBLE_CROSS_PRODUCT_ROWS))
    {
      add_setting(INFEASIBLE_CROSS_PRODUCT_ROWS);
      infeasible_cross_product_rows= other->infeasible_cross_product_rows;
    }
    else if (other->is_nulled(INFEASIBLE_CROSS_PRODUCT_ROWS))
      remove_setting(INFEASIBLE_CROSS_PRODUCT_ROWS);
  }

  /* Time to allow a possibly infeasible query to run before killing it */
  uint infeasible_max_time;

  inline uint get_infeasible_max_time(uint default_val)
  {
    return (has_setting(INFEASIBLE_MAX_TIME)) ? infeasible_max_time : default_val;
  }

  inline void update_infeasible_max_time(struct sniper_settings *other)
  {
    if (other->has_setting(INFEASIBLE_MAX_TIME))
    {
      add_setting(INFEASIBLE_MAX_TIME);
      infeasible_max_time= other->infeasible_max_time;
    }
    else if (other->is_nulled(INFEASIBLE_MAX_TIME))
      remove_setting(INFEASIBLE_MAX_TIME);
  }

  /*
    secondary requirements for the infeasible sniper.
    Stored as uint because of a circular dependency.
  */
  SNIPER_INFEASIBLE_SECONDARY_REQUIREMENTS infeasible_secondary_reqs;

  inline SNIPER_INFEASIBLE_SECONDARY_REQUIREMENTS get_infeasible_secondary_reqs(
          SNIPER_INFEASIBLE_SECONDARY_REQUIREMENTS default_val)
  {
    return (has_setting(INFEASIBLE_SECONDARY_REQS)) ?
        infeasible_secondary_reqs : default_val;
  }

  inline void update_infeasible_secondary_reqs(struct sniper_settings *other)
  {
    if (other->has_setting(INFEASIBLE_SECONDARY_REQS))
    {
      add_setting(INFEASIBLE_SECONDARY_REQS);
      infeasible_secondary_reqs= other->infeasible_secondary_reqs;
    }
    else if (other->is_nulled(INFEASIBLE_SECONDARY_REQS))
      remove_setting(INFEASIBLE_SECONDARY_REQS);
  }

  uint specified_settings;
  // Used by parser to note if settings are explicitly set to null
  uint nulled_settings;

  inline void add_setting(uint setting)
  {
    specified_settings|= setting;
    nulled_settings&= (~setting);
  }
  inline void remove_setting(uint setting)
  {
    specified_settings&= ~setting;
    nulled_settings&= (~setting);
  }
  inline bool has_setting(uint setting) { return specified_settings & setting; }
  inline bool has_any_setting() { return has_setting(~0); }
  inline bool is_nulled(uint setting) { return nulled_settings & setting; }
  inline void null_setting(uint setting)
  {
    nulled_settings|= setting;
    specified_settings&= (~setting);
  }

  inline bool is_setting_used(uint setting)
  {
    return (nulled_settings | specified_settings) & setting;
  }

  inline void update(struct sniper_settings *other)
  {
    update_idle_timeout(other);
    update_long_query_timeout(other);
    update_kill_connectionless(other);
    update_infeasible_cross_product_rows(other);
    update_infeasible_secondary_reqs(other);
    update_infeasible_max_time(other);
  }

  inline void reset()
  {
    bzero(this, sizeof(*this));
  }

} SNIPER_SETTINGS;


#endif  // SNIPER_STRUCTS_INCLUDED
