# Test the the httpd server:
#  check the main page for master and slave
#  check the /var
#  check the /var?var=
#  check the /health
#
# Parameters:
#
# $target
#   A hostname:port or a unix socket.
#

echo Test the main page;

echo Check the master table headers;
exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT
    | egrep " <th>.*</th>" | sed "s/<[^>]*>/ /g";

echo Check the binary log on the master;
exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT
    | grep "master-bin"
    | sed -r -e "s/<[^>]*>/ /g" -e "s/\.[0-9]+//";

echo Check the slave connection on the master;
exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT
    | grep "Binlog Dump"
    | sed "s/<[^>]*>/ /g";

echo Check the slave table headers;
exec $HTTPD_TEST 0.0.0.0:$SLAVE_HTTPD_PORT
    | egrep " <th>.*</th>" | sed "s/<[^>]*>/ /g"
    | sed "s/<[^>]*>/ /g";

echo Check the binary and relay logs on the slave;
exec $HTTPD_TEST 0.0.0.0:$SLAVE_HTTPD_PORT
    | egrep "master-bin|slave-bin|slave-relay-bin"
    | sed -r -e "s/<[^>]*>/ /g" -e "s/\.[0-9]+ \/ [0-9]+//";

echo Check the connection to the master;
exec $HTTPD_TEST 0.0.0.0:$SLAVE_HTTPD_PORT
    | grep "Waiting for master to send event"
    | sed "s/<[^>]*>/ /g";

echo Check for SQL and IO thread;
exec $HTTPD_TEST 0.0.0.0:$SLAVE_HTTPD_PORT
    | grep "system user"
    | sed "s/<[^>]*>/ /g";

echo Test the /var;
exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT var
    | egrep "(status_uptime|status_queries|status_questions) "
    | sed "s/[0-9][0-9]*/NUMBER/g";

exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT var?var=;
exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT var?var=:;
exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT var?var=:::;

exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT var?var=status_uptime
    | sed "s/[0-9][0-9]*/NUMBER/g";

exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT var?var=status_uptime:
    | sed "s/[0-9][0-9]*/NUMBER/g";

exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT var?var=status_uptime:::
    | sed "s/[0-9][0-9]*/NUMBER/g";

exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT var?var=status_uptime:status_queries:status_questions
    | sed "s/[0-9][0-9]*/NUMBER/g";

echo Test the /health;
exec $HTTPD_TEST 0.0.0.0:$MASTER_HTTPD_PORT health;
