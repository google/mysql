
if ($rpl_inited)
{
  if (!$allow_rpl_inited)
  {
    --die ERROR IN TEST: Cannot work when using the replication test framework (master-slave.inc, rpl_init.inc etc).
  }
}

# Write file to make mysql-test-run.pl expect the "crash", but don't start
# it until it's told to
--let $_server_id= `SELECT @@server_id`
--let $_expect_file_name= $MYSQLTEST_VARDIR/tmp/mysqld.$_server_id.expect
--exec echo "wait" > $_expect_file_name

# Send shutdown to the connected server and give
# it 10 seconds to die before zapping it
shutdown_server 10;

# Run requested bootstrap script
if ($bootstrap_error)
{
  --error 1
  --exec $MYSQLD_BOOTSTRAP_CMD < $bootstrap_sql >> $MYSQLTEST_VARDIR/tmp/bootstrap.log 2>&1
}
if (!$bootstrap_error)
{
  --exec $MYSQLD_BOOTSTRAP_CMD < $bootstrap_sql >> $MYSQLTEST_VARDIR/tmp/bootstrap.log 2>&1
}

# Write file to make mysql-test-run.pl start up the server again
--exec echo "restart" > $_expect_file_name

# Turn on reconnect
--enable_reconnect

# Call script that will poll the server waiting for it to be back online again
--source include/wait_until_connected_again.inc

# Turn off reconnect again
--disable_reconnect

