#
# Helper script to delete anonymous users from privilege tables.
# Users must be restored later by reenable_anon_user.inc.
#

connection default;

create table mysql.user_bak like mysql.user;
insert into mysql.user_bak select * from mysql.user where User = '';
delete from mysql.user where User = '';

create table mysql.db_bak like mysql.db;
insert into mysql.db_bak select * from mysql.db where User = '';
delete from mysql.db where User = '';

flush privileges;
