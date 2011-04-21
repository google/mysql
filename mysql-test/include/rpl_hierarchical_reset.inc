# ==== Purpose ====
#
# Called from rpl_end.inc. If servers are running with rpl-hierarchical,
# resets their group_ids back to 0. Needed because the next test may run
# the master with different options and, when that's the case, mtr doesn't
# preserve the data directory, resetting the server's group_id to 0.

--let $include_filename= rpl_hierarchical_reset.inc
--source include/begin_include_file.inc

--disable_query_log

--let $_rpl_hierarchical_value= query_get_value(SHOW VARIABLES LIKE 'rpl_hierarchical', Value, 1)
if (`SELECT '$_rpl_hierarchical_value' = 'ON'`)
{
  --let $_rpl_server= $rpl_server_count
  while ($_rpl_server)
  {
    --let $rpl_connection_name= server_$_rpl_server
    --source include/rpl_connection.inc

    SET BINLOG_GROUP_ID=0, MASTER_SERVER_ID=0 WITH RESET;

    --dec $_rpl_server
  }
}

--let $include_filename= rpl_hierarchical_reset.inc
--source include/end_include_file.inc
