# ==== Purpose ====
#
# Ensure that the server is running with rpl_hierarchical on
#
# ==== Usage ====
#
# source include/have_rpl_hierarchical.inc

-- require r/have_rpl_hierarchical.require
disable_query_log;
show variables like 'rpl_hierarchical';
enable_query_log;
