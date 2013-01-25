# Tests for the effects of sql_select.cc:is_read_clause_safe_for_stmt_replication()
#
# These tests are mostly designed to show that the server is detecting write
# queries where the order of the written rows is non-deterministic, and
# therefore unsafe to use with LIMIT clauses and statement-based replication.

# This produces different output depending on the replication mode,
# so it is included in different test files that require particular modes
--source include/master-slave.inc

--disable_warnings
--disable_query_log
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
--enable_query_log
--enable_warnings

CREATE TABLE t1 (x int PRIMARY KEY, y int, z int);
CREATE TABLE t2 (x int PRIMARY KEY, y int UNIQUE NOT NULL, z int);
CREATE TABLE t3 (a int PRIMARY KEY, b int UNIQUE NULL, c int);
CREATE TABLE t4 (a int, b int, c int, PRIMARY KEY (a,b));
CREATE TABLE t5 (x int PRIMARY KEY, yy int UNIQUE NOT NULL, zz int);
CREATE OR REPLACE VIEW v1 AS SELECT x+5 as x,y,z FROM t2;
CREATE OR REPLACE VIEW v2 AS SELECT x+15 as x,y,z FROM t2 LIMIT 1;
CREATE OR REPLACE VIEW v3 AS SELECT x+25 as x,y,z FROM t2 ORDER BY t2.x;
CREATE OR REPLACE VIEW v4 AS SELECT x+35 as x,y,z FROM t2 ORDER BY t2.x LIMIT 1;

INSERT INTO t2 VALUES (1,1,1), (2,2,2), (3,3,3);
INSERT INTO t3 VALUES (1,0,0), (2,4,5), (3,6,7), (4, NULL, 8), (5, NULL, 9);
INSERT INTO t4 VALUES (1,2,3), (2,2,3), (3,4,4);

--echo NOTE: "Unsafe" means unsafe for statement-mode replication; unsafe
--echo queries should emit warnings during statement-based replication and
--echo use row-based logging in mixed mode replication.
--echo -------------------------------------------------------------------

--echo * SAFE: Queries with neither LIMIT or ORDER BY
INSERT INTO t1 SELECT x,y,z FROM t2;
REPLACE INTO t1 SELECT x,y,z FROM t2 JOIN t3 ON x=a;
INSERT INTO t1 SELECT x+10,y,z FROM (SELECT x,y,z FROM t2) q1;
REPLACE INTO t1 SELECT x,y,z FROM v1;
UPDATE t1 SET z = 10 WHERE x = 3;
INSERT INTO t1 SELECT x+20,y,z FROM t2 UNION SELECT 29,0,0;
DELETE FROM t1;
--echo * UNSAFE: LIMIT without ORDER BY present anywhere is unsafe
INSERT INTO t1 SELECT x,y,z FROM t2 LIMIT 1;
REPLACE INTO t1 SELECT x,y,z FROM v1 LIMIT 1;
DELETE FROM t1 WHERE x < 3 LIMIT 1;
UPDATE t1 SET z = z+10 LIMIT 1;
--echo * SAFE: Queries with both LIMIT and ORDER BY primary key
REPLACE INTO t1 SELECT x,y,z FROM v1 ORDER BY x LIMIT 1;
DELETE FROM t1 WHERE x < 3 ORDER BY t1.x LIMIT 1;
INSERT INTO t1 SELECT x,y,z FROM t2 ORDER BY x LIMIT 1;
UPDATE t1 SET z = 10 ORDER BY X LIMIT 1;
--echo * SAFE: DESC ordering does not affect safety
REPLACE INTO t1 SELECT x,y,z FROM t2 ORDER BY x DESC LIMIT 1;
--echo * SAFE: Redundantly ordering by extra columns does not affect safety
REPLACE INTO t1 SELECT x,y,z FROM t2 ORDER BY x,z LIMIT 1;
REPLACE INTO t1 SELECT x,y,z FROM t2 ORDER BY z,x LIMIT 1;
REPLACE INTO t1 SELECT x,y,z FROM t2 ORDER BY z,y LIMIT 1;
REPLACE INTO t1 SELECT x,y,z FROM t2 ORDER BY z,x,y LIMIT 1;
--echo * SAFE: LIMIT with ORDER BY UNIQUE NOT NULL
REPLACE INTO t1 SELECT x,y,z FROM t2 ORDER BY y LIMIT 1;
--echo * SAFE: Queries on views with both LIMIT and unique ORDER BY
INSERT INTO t1 SELECT x+20,y,z FROM v4;
--echo * SAFE: Views with unique ORDER BY, with LIMIT in query
REPLACE INTO t1 SELECT x,y,z FROM v3 LIMIT 1;
--echo * UNSAFE: View with only LIMIT, queried with ORDER (due to possibly having applied LIMIT before ORDER)
INSERT INTO t1 SELECT x+30,y,z FROM v2 ORDER BY x;
--echo * UNSAFE: LIMIT with ORDER BY only non-unique columns
REPLACE INTO t1 SELECT x,y,z FROM t2 ORDER BY z LIMIT 1;
UPDATE t1 SET z = z+20 ORDER BY Z LIMIT 1;
--echo * UNSAFE: LIMIT with ORDER BY UNIQUE NULL columns is unsafe (they don't actually enforce row uniqueness)
INSERT INTO t1 SELECT a+40,b,c FROM t3 ORDER BY b LIMIT 1;
--echo * UNSAFE: Unsafe queries within UNION
REPLACE INTO t1 SELECT x,y,z FROM t2 LIMIT 1 UNION SELECT 9,0,0;
--echo * SAFE: UNION of safe queries without LIMIT outside of the union branches
INSERT INTO t1 (SELECT x+50,y,z FROM t2 ORDER BY x LIMIT 1) UNION (SELECT 59,0,0);
--echo * UNSAFE UNION with LIMIT
REPLACE INTO t1 (SELECT x,y,z FROM t2) UNION (SELECT 9,0,0) LIMIT 1;
--echo * UNSAFE: UNION with LIMIT, even when contained selects use ORDER BY
INSERT INTO t1 (SELECT x+60,y,z FROM t2 ORDER BY x) UNION (SELECT 69,0,0) LIMIT 1;
--echo * UNSAFE: UNION with LIMIT and ORDER BY (is 1,1,1 or 1,0,0 first?)
REPLACE INTO t1 (SELECT x,y,z FROM t2) UNION (SELECT 1,0,0) ORDER BY x LIMIT 1;
--echo * UNSAFE: Unsafe subqueries
INSERT INTO t1 SELECT x+70,y,z FROM (SELECT x,y,z FROM t2 LIMIT 1) q1;
--error ER_NOT_SUPPORTED_YET # remove when this works; should then be unsafe
UPDATE t1 SET z=z+40 WHERE x IN (SELECT x FROM t2 LIMIT 1);
--echo * UNSAFE: Views with embedded LIMIT but not ORDER BY
INSERT INTO t1 SELECT x+80,y,z FROM v2;
UPDATE t1 SET z=z+80 WHERE x IN (SELECT x FROM v2);
--echo * UNSAFE: Unsafe views in subqueries
DELETE FROM t1 WHERE x IN (SELECT x+2 FROM v2);
--echo * SAFE: Ordered subqueries with LIMIT on outer query
REPLACE INTO t1 SELECT xx,y,z FROM (SELECT x AS xx,y,z FROM t2 ORDER BY x) q1 LIMIT 1;
REPLACE INTO t1 SELECT xx,y,z FROM (SELECT x AS xx,y,z FROM t2 ORDER BY x LIMIT 1) q1;
--echo * UNSAFE: Unordered subqueries with LIMIT on outer query
REPLACE INTO t1 SELECT xx,y,z FROM (SELECT x AS xx,y,z FROM t2) q1 LIMIT 1;
--echo * UNSAFE: UNIONS as subqueries with LIMIT on outer query
INSERT INTO t1 SELECT x+90,y,z FROM ((SELECT x,y,z FROM t2 ORDER BY x) UNION (SELECT 1,2,3)) q1 LIMIT 1;
--echo * SAFE: Ordering by multi-column keys is safe, regardless of column precedence
REPLACE INTO t1 SELECT a,b,c FROM t4 ORDER BY a,b LIMIT 1;
REPLACE INTO t1 SELECT a,b,c FROM t4 ORDER BY b,a LIMIT 1;
--echo * UNSAFE: Multi-column keys must have conditions on all columns
INSERT INTO t1 SELECT a+100,b,c FROM t4 ORDER BY a LIMIT 1;
REPLACE INTO t1 SELECT a,b,c FROM t4 ORDER BY b LIMIT 1;
--echo * SAFE: LIMIT on joins where ORDER BY uniquely orders all tables
REPLACE INTO t1 SELECT x,y,z FROM t2 JOIN t3 ON x=a ORDER BY x, a LIMIT 1;
INSERT INTO t1 SELECT x+1000,y,z FROM t2 NATURAL JOIN t5 ORDER BY t2.x, t5.x LIMIT 1;
--echo * UNSAFE: LIMIT on joins where ORDER BY doesn't uniquely order some of the joined tables
REPLACE INTO t1 SELECT x,y,z FROM t2 JOIN t3 ON x=b ORDER BY x LIMIT 1;
--echo * FALSE ALARM: Natural join columns aren't resolved correctly pre-fix_fields()
INSERT INTO t1 SELECT x+1000,y,z FROM t2 NATURAL JOIN t5 ORDER BY x LIMIT 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP VIEW v1;
DROP VIEW v2;
DROP VIEW v3;
DROP VIEW v4;

--source include/show_binlog_events.inc
--source include/rpl_end.inc
