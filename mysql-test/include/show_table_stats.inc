eval create table t1 (a int not null primary key, b int not null) engine=$engine_type;
eval create table t2 (a int not null primary key, b int not null) engine=$engine_type;
eval create table t3 (a int not null primary key, b int not null, index bx(b)) engine=$engine_type;


echo "";
echo Test insert;
echo "";

flush table_statistics;
insert into t1 values (1,1), (2,2);

echo test.t1 should have 0 read, 2 changed;
show table_statistics;
select * from INFORMATION_SCHEMA.TABLE_STATISTICS order by TABLE_NAME;

echo "";
echo Test insert select;
echo "";

flush table_statistics;
insert into t2 select * from t1;

echo test.t1 should have 2 read, 0 changed;
echo test.t2 should have 0 read, 2 changed;
show table_statistics;
select * from INFORMATION_SCHEMA.TABLE_STATISTICS order by TABLE_NAME;

echo "";
echo Test update with PK scans;
echo "";

insert into t1 values (0,0);
select count(*) from t1;

flush table_statistics;
update t1 set a = 3 where a = 1;
update t1 set a = 4 where b = 2;

echo test.t1 should have 4 (MyISAM) or 5 (InnoDB) read, 2 changed;
show table_statistics;
select * from INFORMATION_SCHEMA.TABLE_STATISTICS order by TABLE_NAME;

echo "";
echo Test update with secondary index scans;
echo "";

insert into t3 values (0,0);
insert into t3 values (1,1);
insert into t3 values (2,2);
flush table_statistics;

update t3 set a = 4 where b = 1;

echo test.t3 should have 1 (MyISAM) or 2 (InnoDB) read, 1 changed;
show table_statistics;
select * from INFORMATION_SCHEMA.TABLE_STATISTICS order by TABLE_NAME;

echo "";
echo Test delete with PK scans;
echo "";

flush table_statistics;
delete from t1 where a = 3;
delete from t1 where b = 2;

echo test.t1 should have 3 read, 2 changed;
show table_statistics;
select * from INFORMATION_SCHEMA.TABLE_STATISTICS order by TABLE_NAME;

echo "";
echo Test delete with secondary index scans;
echo "";

flush table_statistics;
delete from t3 where b = 1;

echo test.t3 should have 1 read, 1 changed;
show table_statistics;
select * from INFORMATION_SCHEMA.TABLE_STATISTICS order by TABLE_NAME;

echo "";
echo Test select with PK scans;
echo "";

insert into t1 values (1,1), (2,2);

flush table_statistics;
select count(*) from t1 where a = 1;
select count(*) from t1 where b < 0;

echo test.t1 should have 4 read, 0 changed;
show table_statistics;
select * from INFORMATION_SCHEMA.TABLE_STATISTICS order by TABLE_NAME;

echo "";
echo Test select with secondary scans;
echo "";

insert into t3 values (1,1);

flush table_statistics;
select * from t3 where b = 1;
select * from t3 where b < 0;

echo test.t3 should have 2 (MyISAM) or 4 (InnoDB) read, 0 changed;
show table_statistics;
select * from INFORMATION_SCHEMA.TABLE_STATISTICS order by TABLE_NAME;

echo "";
echo Test temporary table;
echo "";

flush table_statistics;
--let i= 4
while ($i)
{
  eval create temporary table aa (a int not null primary key) engine=$engine_type;
  insert into aa values (1), (2), (3), (4);
  drop table aa;
  --dec $i
}

echo test.aa.root should have 0 read, 16 changed;
show table_statistics;
select * from INFORMATION_SCHEMA.TABLE_STATISTICS order by TABLE_NAME;

# Clean up
drop table t1;
drop table t2;
drop table t3;
