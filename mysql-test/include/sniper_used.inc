# include/sniper_used.inc
#
# SUMMARY
#
# Adds mtr log expectations for snipers
#
# USAGE
#
#    let $sniper_kills= 5;
#    --source include/sniper_used.inc
#
# EXAMPLE
#    sniper_idle.test
#

--disable_query_log
--disable_warnings

eval call mtr.expect_log_output("Sniper: Attempting to kill THD id=[0-9]+", $sniper_kills);

--enable_warnings
--enable_query_log
