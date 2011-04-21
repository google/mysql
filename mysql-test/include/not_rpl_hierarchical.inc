# ==== Purpose ====
#
# Ensure that the server is not running with rpl_hierarchical on
#
# ==== Usage ====
#
# source include/not_rpl_hierarchical.inc

-- require r/not_rpl_hierarchical.require
disable_query_log;
show variables like 'rpl_hierarchical';
enable_query_log;
