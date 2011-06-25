# ==== Purpose ====
#
# Ensure that the server is running with rpl_event_checksums on
#
# ==== Usage ====
#
# source include/have_event_checksums.inc

-- require r/have_event_checksums.require
disable_query_log;
show variables like 'rpl_event_checksums';
enable_query_log;
