
------------------------------------------------------------------------------------------------------
-- SETUP STEPS
------------------------------------------------------------------------------------------------------
--
-- 2. Unzip the 2008_cms_data.csv.tar.gz via 'tar -xvf 2008_cms_data_csv.tar.gz'
-- 3. Use Appendix A of this script to create the two custom views that show storage and compression data.
-- 4. Use Appendix B of this script to create the 'example-plpgsql.sql' file.
-- 5. The CMS dataset has only 9M records.  Use Appendix C of this script to increase the data volume as you wish.
-- 6. Use a tool like pgAdmin3 or DbVisualizer to execute the statements in this script during the demo.


--------------------------------------------------------------------------------------
-- PART I - LOADING DATA.  ALSO A QUICK INTRO TO PSQL.
--------------------------------------------------------------------------------------
-- create a database to work in.
create database ditl;

-- Drop these objects if they already exist in the database.
drop table if exists cms;
drop table if exists cms_part;
drop table if exists cms_qlz;
drop table if exists cms_zlib;
drop table if exists cms_zlib9;
drop table if exists wwearthquakes_lastwk;
drop table if exists cms_load_errors;
drop table if exists cms_bad_key;
drop external table if exists cms_backup;
drop external table if exists cms_export;
drop external table if exists ext_cms;
drop external table if exists ext_wwearthquakes_lastwk;
drop table if exists cms_seq;
drop table if exists cms_p0;
drop sequence if exists myseq;

-- Create the table to hold the cms data from data.gov.  we already know the layout.
drop table if exists cms;
CREATE TABLE cms
(
  car_line_id character varying(20),
  bene_sex_ident_cd numeric(20),
  bene_age_cat_cd bigint,
  car_line_icd9_dgns_cd character varying(10),
  car_line_hcpcs_cd character varying(10),
  car_line_betos_cd character varying(5),
  car_line_srvc_cnt bigint,
  car_line_prvdr_type_cd bigint,
  car_line_cms_type_srvc_cd character varying(5),
  car_line_place_of_srvc_cd bigint,
  car_hcpcs_pmt_amt bigint
)
distributed by (car_line_id);

-- Connect to the dca and database through a terminal window.
-- skip this if connecting locally.
ssh gpadmin@10.5.80.41  --supply password if prompted


-- Some tasks to demo typical stuff for command-line lovers.
psql -d ditl
\?, \h, \h vacuum
\d, \d cms, \dv, \d v_gp
\l
\i /home/gpadmin/GPDB_Examples/gpdb-dayinthelife/plpgsql.sql  -- execute a sql script to create a pl/pgsql function
select myfunc (3, 'Hello');  -- test the function.  try it with 7 and 20.

-- Back to loading data.
-- Use the 'COPY' command in the command line to bulk-load data via the master server.
-- This will take 5 minutes on a single node VM.  It will take 70 seconds on a 1/4-rack DCA.
\timing on
\COPY cms FROM '/home/gpadmin/GPDB_Examples/gpdb-dayinthelife/2008_cms_data.csv' CSV HEADER LOG ERRORS INTO cms_load_errors KEEP SEGMENT REJECT LIMIT 50 ROWS;  -- (75 secs.)
select count(*) total_records from cms;
truncate table cms;

-- Kill and restart the gpfdist utility on the database
ps ax | grep gpfdist
pkill -9 gpfdist
gpfdist -d /home/gpadmin/GPDB_Examples/gpdb-dayinthelife/ -p 8081 -l /home/gpadmin/GPDB_Examples/gpdb-dayinthelife/gpfdist.log &


-- Create an external table that 'points' to the source file.
drop external table if exists ext_cms;
create external table ext_cms (like cms) location ('gpfdist://localhost:8081/2008_cms_data.csv') format 'csv' (header);
-- NOTE:  can load multiple zipped files in parallel without unzipping.  gpfdist://mdw:8081/*.gz'   VERY FAST!) 

-- View the data to see there are no tricks.
select count(*) total_recs from cms;
select count(*) total_recs from ext_cms;  -- (11 secs.  --only works if the file is unzipped)

-- Load the data from the source file (external table) into the database table.
insert into cms (select * from ext_cms);  -- (25 seconds on 1/4 rack DCA; 5 minutes on a single-node VM)
select count(*) total_recs from cms;


-- Data load from external web source - Worldwide M1+ earthquakes for the last 7 days
-- https://explore.data.gov/Geography-and-Environment/Worldwide-M1-Earthquakes-Past-7-Days/7tag-iwnu
DROP TABLE IF EXISTS WWearthquakes_lastWk;
CREATE TABLE WWearthquakes_lastWk (
time TEXT, latitude numeric, longitude numeric, depth numeric, mag numeric, mag_type varchar (10),
NST integer, gap numeric, dmin numeric, rms text, net text, id text, updated TEXT, place varchar(150), type varchar(50)
)
DISTRIBUTED BY (time);

DROP EXTERNAL TABLE IF EXISTS ext_WWearthquakes_lastWk;
create external web table ext_WWearthquakes_lastWk (like WWearthquakes_lastWk) 
Execute 'wget -qO - http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.csv'  -- defining an OS command to execute
ON ALL
Format 'CSV' (HEADER)
LOG ERRORS INTO err_earthquakes
Segment Reject limit 300;

select count(*) from ext_wwearthquakes_lastwk;

-- Load the data into the table
Insert into WWearthquakes_lastWk select * from ext_WWearthquakes_lastWk;

-- Browse the data.
select count(*) from wwearthquakes_lastwk;
select * from WWearthquakes_lastWk order by mag desc limit 100;


-- Using a Sequence during load.  Take note of the 'blocks of values' assigned.  The master is the single source of truth.
drop sequence if exists myseq;
create sequence myseq start 300 cache 1000;

drop table if exists cms_seq;
create table cms_seq as (select nextval('myseq') as my_key, car_line_id, bene_sex_ident_cd, bene_age_cat_cd from cms limit 100);

select * from cms_seq order by 1;


-- Always a good idea to run VACUUM & ANALYZE during normal maintenance or after big loads.
-- Can alternatively use pgAdmin3 GUI interface to perform the task.
vacuum analyze cms;  


--------------------------------------------------------------------------------------
-- PART II - DATA DISTRIBUTION, PARTITIONING, AND POLYMORPHIC STORAGE.
--------------------------------------------------------------------------------------
-- Create a table with a bad distribution key.
drop table if exists cms_bad_key;
create table cms_bad_key (like cms) distributed by (bene_sex_ident_cd);

-- Load data into the table and browse.
insert into cms_bad_key (select * from cms);  -- 140 seconds on a 1/4 rack; 5 minutes on single-node VM)
select count(*) total_records from cms_bad_key;

-- View skew.  Could also be done live in Command Center in real time.
Look at 'gp_skew_coefficients' in gp_toolkit.  Lower number is better.
Look at 'gp_skew_idle_fractions' in gp_toolkit.  0.1 = 10% idle, which is ok.  0.5 = 50%, which is bad.
select distinct (bene_sex_ident_cd) from cms;   -- A binary key will result in highly skewed data load (compare to cms)

-- Create a partitioned table
drop table if exists cms_part;
create table cms_part(
  car_line_id character varying(20),
  bene_sex_ident_cd numeric(20,0),
  bene_age_cat_cd bigint,
  car_line_icd9_dgns_cd character varying(10),
  car_line_hcpcs_cd character varying(10),
  car_line_betos_cd character varying(5),
  car_line_srvc_cnt bigint,
  car_line_prvdr_type_cd bigint,
  car_line_cms_type_srvc_cd character varying(5),
  car_line_place_of_srvc_cd bigint,
  car_hcpcs_pmt_amt bigint
)
distributed by (car_line_id)
partition by list (car_line_cms_type_srvc_cd) 
	(partition p1 values ('0'), partition p2 values ('1'), partition p3 values ('2'), partition p4 values ('3'),
	partition p5 values ('4'), partition p6 values ('5'), partition p7 values ('6'), partition p8 values ('7'),
	partition p9 values ('8'), partition p10 values ('9'), partition p11 values ('D'), partition p12 values ('F'),
	partition p13 values ('G'), partition p14 values ('K'), partition p15 values ('M'), partition p16 values ('N'),
	default partition other );

-- Load data into the partitioned table and browse the counts.
insert into cms_part (select * from cms);  -- (15 secs.)

select count(*) total_records from cms_part;

-- Generate an explain plan to see the cost of the query on the non-partitioned table
explain
select cms.*
from cms
where car_line_cms_type_srvc_cd in ('N', '0', 'G', 'S')
order by car_line_id;  -- (3.4 secs)

-- Generate an explain plan to see the cost of the query on the partitioned table
explain
select cms_part.*
from cms_part
where car_line_cms_type_srvc_cd in ('N', '0', 'G', 'S')
order by car_line_id; -- (1 sec.)


-- Storage and Compression
-- Create the table that's column-oriented with quickLZ compression
drop table if exists cms_qlz;
create table cms_qlz (like cms)
with (appendonly=true, orientation=column, compresstype=quicklz)
PARTITION BY LIST (car_line_cms_type_srvc_cd) 
	(PARTITION p1 values ('0'), PARTITION p2 values ('1'), PARTITION p3 values ('2'), PARTITION p4 values ('3'),
	PARTITION p5 values ('4'), PARTITION p6 values ('5'), PARTITION p7 values ('6'), PARTITION p8 values ('7'),
	PARTITION p9 values ('8'), PARTITION p10 values ('9'), PARTITION p11 values ('D'), PARTITION p12 values ('F'),
	PARTITION p13 values ('G'), PARTITION p14 values ('K'), PARTITION p15 values ('M'), PARTITION p16 values ('N'),
	default partition other );

insert into cms_qlz (select * from cms);


-- Create a table with zLib, level=5
drop table if exists cms_zlib;
create table cms_zlib (like cms)
with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
PARTITION BY LIST (car_line_cms_type_srvc_cd) 
	(PARTITION p1 values ('0'), PARTITION p2 values ('1'), PARTITION p3 values ('2'), PARTITION p4 values ('3'),
	PARTITION p5 values ('4'), PARTITION p6 values ('5'), PARTITION p7 values ('6'), PARTITION p8 values ('7'),
	PARTITION p9 values ('8'), PARTITION p10 values ('9'), PARTITION p11 values ('D'), PARTITION p12 values ('F'),
	PARTITION p13 values ('G'), PARTITION p14 values ('K'), PARTITION p15 values ('M'), PARTITION p16 values ('N'),
	default partition other );

-- Load the data.
insert into cms_zlib (select * from cms);  --(11 secs.)
select count(*) as total_recs from cms_zlib;


-- Create a table with zLib, level=9
drop table if exists cms_zlib9;
create table cms_zlib9 (like cms)
with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=9)
PARTITION BY LIST (car_line_cms_type_srvc_cd) 
	(PARTITION p1 values ('0'), PARTITION p2 values ('1'), PARTITION p3 values ('2'), PARTITION p4 values ('3'),
	PARTITION p5 values ('4'), PARTITION p6 values ('5'), PARTITION p7 values ('6'), PARTITION p8 values ('7'),
	PARTITION p9 values ('8'), PARTITION p10 values ('9'), PARTITION p11 values ('D'), PARTITION p12 values ('F'),
	PARTITION p13 values ('G'), PARTITION p14 values ('K'), PARTITION p15 values ('M'), PARTITION p16 values ('N'),
	default partition other );

-- Load the data.
insert into cms_zlib9 (select * from cms);  --(65 secs.)
select count(*) total_recs from cms_zlib9;


-- With all the same counts, look at the size of tables and indexes.  Note:  No Indexes!!!
SELECT tabs.nspname AS schema_name
,      COALESCE(parts.tablename, tabs.relname) AS table_name
,      ROUND(SUM(sotaidtablesize)/1024/1024/1024,3) AS table_GB
,      ROUND(SUM(sotaididxsize)/1024/1024/1024,3) AS index_GB
FROM   gp_toolkit.gp_size_of_table_and_indexes_disk sotd
,     (SELECT c.oid, c.relname, n.nspname
       FROM   pg_class c
       ,      pg_namespace n
       WHERE  n.oid = c.relnamespace
       AND    c.relname NOT LIKE '%_err'
      )tabs
LEFT JOIN pg_partitions parts
ON     tabs.nspname = parts.schemaname
AND    tabs.relname = parts.partitiontablename
WHERE  sotd.sotaidoid = tabs.oid and tabs.nspname = 'public'
GROUP BY tabs.nspname, COALESCE(parts.tablename, tabs.relname)
ORDER BY 1 desc,2;

-- View this customized view for table, storage, and compression characteristics.
select * from v_gp_table_storage2;


-- Example of swapping partitions.
-- create a table that represents one of the partitions and load it;
drop table if exists cms_p0;
create table cms_p0 (like cms_part) with (appendonly=true, compresstype=quicklz, orientation=column);

insert into cms_p0 (select * from cms where car_line_cms_type_srvc_cd = 'M');
select count(*) total_records from cms_p0;

-- swap the partition in the table with our 'latest' table.
alter table cms_part EXCHANGE PARTITION FOR ('M') WITH TABLE cms_p0;
select count(*) total_records from cms_part where car_line_cms_type_srvc_cd = 'M';


-- Perform some garbage-collection and cleanup.  -- (87 secs.)
vacuum analyze cms;
vacuum analyze cms_part;
vacuum analyze cms_qlz;
vacuum analyze cms_zlib;
vacuum analyze cms_zlib9;


-----------------------------------------------
-- PART III - BACKUP OPTIONS
-----------------------------------------------

-- Use a writable external tables for controlled backup.
-- Create an external table that we can write data to (but really its a file).
drop external table if exists cms_export;
create writable external table cms_export (like cms)
location ('gpfdist://localhost:8081/cms_backup.csv')
format 'csv' (delimiter ',' null '');

-- Load data into the external table (flat file).
insert into cms_export (select * from cms limit 25000); --(7 secs.)
select count(*) from cms_export;  --you get an error, but you can see the file in the directory.


-- Re-load the data from the backup.  Start with creating an external table.
drop external table if exists cms_backup;
create external table cms_backup (like cms)
location ('gpfdist://localhost:8081/cms_backup.csv')
format 'csv' (delimiter ',' null '');

select count(*) total_records from cms_backup;
select count(*) total_records from cms;
insert into cms (select * from cms_backup);

-- Options to backup from the command line.
pg_dump -t cms_seq ditl > backup1.sql;  --basic utlity that creates one file on the server.  Loaded via COPY.
pg_dump --column-inserts -t cms_seq ditl > backup2.sql --option to include all insert statements.
-- pg_restore is used to restore the database.  Has many options as well.


----------------------------------------------------------------------------
-- PART IV - BI INTEROPERABILITY. 
-- REFERENCE THE GPDB-PENTAHO INTEROPERABILITY VIDEO ON GREENPLUM NATION.
----------------------------------------------------------------------------



-------------------------------------------------------------------------------------------------------------------------------
-- PART V - MADLIB EXAMPLE.  CALLING LINEAR REGRESSION IN-LINE SQL.  FRAME A QUESTION THAT CAN'T BE ANSWERED BY SIMPLE BI.
-- WORK-IN-PROGRESS  
-------------------------------------------------------------------------------------------------------------------------------
select (madlib.linregr(car_line_place_of_srvc_cd, array[1, car_line_srvc_cnt, bene_age_cat_cd])).* from cms;



--------------------------------------------------------------
-- PART VI - ALPINE MINER EXAMPLE.  
---------------------------------------------------------------



----------------------------------------------------------------------------
-- APPENDIX A:  CREATING CUSTOM VIEWS FOR TABLE STORAGE.
----------------------------------------------------------------------------

-- Custom view for table storage
CREATE OR REPLACE VIEW public.v_gp_table_storage AS
SELECT current_timestamp AS tms, n.nspname AS schema_name, c.relname AS table_name,
        CASE
            WHEN c.relstorage = 'a'::"char" THEN 'row append-only'::text
            WHEN c.relstorage = 'c'::"char" THEN 'column append-only'::text
            WHEN c.relstorage = 'h'::"char" THEN 'heap'::text
            WHEN c.relstorage = 'x'::"char" THEN 'external'::text
            ELSE NULL::text
        END AS storage_type,
              a.compresstype  AS compr_type,
              a.compresslevel AS compr_level,
              sotailtablesizedisk                                   as tabind_sz,
              (sotailtablesizedisk         / 1024^3)::numeric(20,2) as tabind_sz_gb,
              (sotailtablesizeuncompressed / 1024^3)::numeric(20,2) as tabind_sz_unc_gb,
              case WHEN (sotailtablesizedisk=0 or sotailtablesizedisk is null) THEN -1 ELSE (sotailtablesizeuncompressed/sotailtablesizedisk)::numeric(6,1) END as compr_ratio
              , c.relhassubclass as is_partitioned
   FROM pg_class c
   LEFT JOIN pg_appendonly a ON c.oid = a.relid
   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
   LEFT JOIN gp_toolkit.gp_size_of_table_and_indexes_licensing sot ON sot.sotailoid = c.oid
  WHERE (n.nspname <> ALL (ARRAY['information_schema'::name, 'pg_catalog'::name, 'pg_toast'::name, 'gp_toolkit'::name])) AND c.relkind = 'r'::"char"
;

-- Another custom view for table storage.  Requires the first custom view.
CREATE OR REPLACE VIEW public.v_gp_table_storage2 AS
 SELECT tms,
        schema_name,
        regexp_replace(table_name::text, '_1_prt_.*$'::text, ''::text) AS table_name,
              storage_type,
              compr_type,
              compr_level,
              count(*)                            AS nr_of_partitions,
              sum(tabind_sz)                      AS tabind_size,
              sum(tabind_sz_gb)                   AS tabind_sz_gb,
              sum(tabind_sz_unc_gb)               AS tabind_sz_unc_gb,
              round(avg(compr_ratio)::numeric, 2) AS avg_compr_ratio
   FROM public.v_gp_table_storage
  WHERE storage_type <> 'external'::text AND table_name !~~ 'err_%'::text AND not is_partitioned
  AND (COMPR_TYPE IS NOT NULL OR TABLE_NAME='cms')
  GROUP BY tms, schema_name, regexp_replace(table_name::text, '_1_prt_.*$'::text, ''::text), storage_type, compr_type, compr_level
  ORDER BY 3;


--------------------------------------------------------------------------------------------
-- APPENDIX B:  SIMPLE PL/PGSQL FUNCTION.  SAVE THIS TO A .SQL FILE AND PUT ONTO THE DCA.
--------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION myFunc (numtimes integer, msg text)
  RETURNS text AS
$BODY$
DECLARE
    strresult text;
BEGIN
    strresult := '';
    IF numtimes = 1 THEN
        strresult := 'Only one row!';
    ELSIF numtimes > 0 AND numtimes < 11 THEN
        FOR i IN 1 .. numtimes LOOP
            strresult := strresult || msg || '; '; --E'\r\n';
        END LOOP;
    ELSE
        strresult := 'You can not do that.';
        IF numtimes <= 0 THEN
            strresult := strresult || ' Must be greater than zero.';
        ELSIF numtimes > 10 THEN
            strresult := strresult || ' That''s too many items!';
        END IF;
    END IF;
    RETURN strresult;
END;
$BODY$
  LANGUAGE 'plpgsql' IMMUTABLE;
ALTER FUNCTION myFunc(integer, text) OWNER TO gpadmin;


----------------------------------------------------------------------
-- APPENDIX C:  MAKING THE CMS DATA LARGER (FROM 9M TO 67M).
----------------------------------------------------------------------
drop table if exists cms;
CREATE TABLE cms
(
  car_line_id character varying(20),
  bene_sex_ident_cd numeric(20),
  bene_age_cat_cd bigint,
  car_line_icd9_dgns_cd character varying(10),
  car_line_hcpcs_cd character varying(10),
  car_line_betos_cd character varying(5),
  car_line_srvc_cnt bigint,
  car_line_prvdr_type_cd bigint,
  car_line_cms_type_srvc_cd character varying(5),
  car_line_place_of_srvc_cd bigint,
  car_hcpcs_pmt_amt bigint
)
distributed by (car_line_id);

create external table ext_cms (like cms) location ('gpfdist://mdw:8081/2008_cms_data.tar.csv.gz') format 'csv' (header);

-- NOTE:  EXECUTE THIS STATEMENT 7 TIMES IN A ROW
insert into cms (select * from ext_cms);  

drop external table if exists cms_export;
create writable external table cms_export (like cms)
location ('gpfdist://mdw:8081/cms_export.csv')
format 'csv' (delimiter ',' null '');


-------------------------------------------
---- APPENDIX D:  RESOURCE QUEUES ---------
-------------------------------------------
-- in gp_toolkit schema
-- gp_resq_priority_statement shows what's currently running
-- gp_locks_on_resqueue


-- QUEUE LIMITS
alter role user1 with resource queue none;
alter role user2 with resource queue none;
drop role if exists user1;
drop role if exists user2;

-- create resource queue with 2 active statements
drop resource queue q_activelimit;
create resource queue q_activelimit with (active_statements = 3);

-- create a user and assign them to a resource queue
create role user1 with login password 'user1';
alter user user1 with resource queue q_activelimit;
alter user user1 set search_path to public, gp_toolkit;
grant usage on schema public to user1;

-- cat small_query, big query, active1-3
-- active1.sh:  small query (7 secs)
-- active2.sh:  (5) small query (15 secs)
-- active3.sh   (10) small qeury (23 secs)
-- active4.sh   (5) small query, but only 3 run at once (check views)


-- create resource queue based on cost
drop resource queue q_costlimit;
create resource queue q_costlimit with (max_cost=14e+6);  --14,000,000 cost limit

-- create a user and assign them to a resource queue
create role user2 with login password 'user2';
alter user user2 with resource queue q_costlimit;
alter user user2 set search_path to public, gp_toolkit;
grant usage on schema public to user2;

-- cost1.sh:    small query (7 secs) --show explain plan, only 1 can fit in the queue at once.
-- cost2.sh:    (3) small query (runs one at a time)
-- cost3.sh:    big query is beyond the cost limit, so you get error message  


-- QUEUE PRIORITIES
create resource queue q_prioritymax with (active_statements=20, priority=max);
create resource queue q_prioritymed with (active_statements=20, priority=medium);
create resource queue q_prioritymin with (active_statements=20, priority=min);

create role usermax with login password 'usermax';
alter user usermax with resource queue q_prioritymax;
alter user usermax set search_path to public, gp_toolkit;
grant usage on schema public to usermax;

create role usermed with login password 'usermed';
alter user usermed with resource queue q_prioritymed;
alter user usermed set search_path to public, gp_toolkit;
grant usage on schema public to usermed;

create role usermin with login password 'usermin';
alter user usermin with resource queue q_prioritymin;
alter user usermin set search_path to public, gp_toolkit;
grant usage on schema public to usermin;

-- sh pri1.sh  -- notice same run-time on each run
-- sh pri2.sh  -- run same query on medium queue (note same timing regardless of queue)
-- sh pri3.sh  -- same on max queue

-- sh pri4.sh  -- run a mix of 3 med, 3 max;  max gets done faster; overall getting done faster (all small query)
-- sh pri5.sh  -- run slow query on min queue, then check CC.  then launch pri4 again. (times wont' be affected)

