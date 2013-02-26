#
# Helper script to add anonymous users back into privilege tables.
# Users must have been removed by disable_anon_user.inc.
#

connection default;

insert into mysql.db select * from mysql.db_bak;
drop table mysql.db_bak;

insert into mysql.user select * from mysql.user_bak;
drop table mysql.user_bak;

flush privileges;
