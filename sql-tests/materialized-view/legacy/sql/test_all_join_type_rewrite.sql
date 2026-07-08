-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Test Objective:
-- 1. Validate MV rewrite coverage across different join semantics.
-- 2. Cover join-type-specific rewrite planning and final query correctness.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_rewrite

-- query 1
CREATE TABLE emps (
    empid INT NOT NULL,
    deptno INT NOT NULL,
    locationid INT NOT NULL,
    commission INT NOT NULL,
    name VARCHAR(20) NOT NULL,
    salary DECIMAL(18, 2)
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
CREATE TABLE depts(
    deptno INT NOT NULL,
    name VARCHAR(20)
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
CREATE TABLE dependents(
    empid INT NOT NULL,
    name VARCHAR(20)
)
TBLPROPERTIES ("format-version" = "3");

-- query 4
insert into emps values(1, 1, 1, 10, "emp_name1", 1000);

-- query 5
insert into emps values(2, 1, 1, 10, "emp_name1", 1000);

-- query 6
insert into emps values(3, 1, 1, 10, "emp_name1", 1000);

-- query 7
insert into depts values(1, "dept_name1");

-- query 8
insert into depts values(2, "dept_name2");

-- query 9
insert into dependents values(1, "dependents_name1");

-- query 10
insert into dependents values(2, "dependents_name2");

-- query 11
insert into dependents values(3, "dependents_name3");

-- query 12
create materialized view mv_right_outer
distributed by hash(`empid`) buckets 10
refresh manual
as
select empid, deptno
from emps right outer join depts using (deptno);

-- query 13
refresh materialized view mv_right_outer with sync mode;

-- query 14
explain logical select empid, deptno
from emps right outer join depts using (deptno);

-- query 15
select empid, deptno
from emps right outer join depts using (deptno) order by empid;

-- query 16
drop materialized view mv_right_outer;

-- query 17
create materialized view mv_full_outer
distributed by hash(`empid`) buckets 10
refresh manual
as
select empid, deptno
from emps full outer join depts using (deptno);

-- query 18
refresh materialized view mv_full_outer with sync mode;

-- query 19
explain logical select empid, deptno
from emps full outer join depts using (deptno);

-- query 20
select empid, deptno
from emps full outer join depts using (deptno) order by empid;

-- query 21
drop materialized view mv_full_outer;

-- query 22
create materialized view mv_left_semi
distributed by hash(`empid`) buckets 10
refresh manual
as
select empid
from emps left semi join depts using (deptno);

-- query 23
refresh materialized view mv_left_semi with sync mode;

-- query 24
explain logical select empid
from emps left semi join depts using (deptno);

-- query 25
select empid
from emps left semi join depts using (deptno) order by empid;

-- query 26
drop materialized view mv_left_semi;

-- query 27
create materialized view mv_left_anti
distributed by hash(`empid`) buckets 10
refresh manual
as
select empid
from emps left anti join depts using (deptno);

-- query 28
refresh materialized view mv_left_anti with sync mode;

-- query 29
explain logical select empid
from emps left anti join depts using (deptno);

-- query 30
select empid
from emps left anti join depts using (deptno) order by empid;

-- query 31
drop materialized view mv_left_anti;

-- query 32
create materialized view mv_right_semi
distributed by hash(`deptno`) buckets 10
refresh manual
as
select deptno
from emps right semi join depts using (deptno);

-- query 33
refresh materialized view mv_right_semi with sync mode;

-- query 34
-- @skip_result_check=true
explain select deptno
from emps right semi join depts using (deptno);

-- query 35
select deptno
from emps right semi join depts using (deptno) order by deptno;

-- query 36
drop materialized view mv_right_semi;

-- query 37
create materialized view mv_right_anti
distributed by hash(`deptno`) buckets 10
refresh manual
as
select deptno
from emps right anti join depts using (deptno);

-- query 38
refresh materialized view mv_right_anti with sync mode;

-- query 39
-- @skip_result_check=true
explain select deptno
from emps right anti join depts using (deptno);

-- query 40
select deptno
from emps right anti join depts using (deptno);
