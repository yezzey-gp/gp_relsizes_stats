/* gp_relsizes_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relsizes_stats" to load this file. \quit


-- CREATE TABLE IF NOT EXISTS ... (....) DISTRIBUTED BY ...
CREATE SCHEMA IF NOT EXISTS relsizes_stats_schema;

-- create table
CREATE TABLE IF NOT EXISTS relsizes_stats_schema.segment_file_map
    (segment INTEGER, reloid OID, relfilenode OID)
    WITH (appendonly=true) DISTRIBUTED RANDOMLY;
-- create table
CREATE TABLE IF NOT EXISTS relsizes_stats_schema.segment_file_sizes
    (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
    WITH (appendonly=true, OIDS=FALSE) DISTRIBUTED RANDOMLY;
TRUNCATE TABLE relsizes_stats_schema.segment_file_sizes;


-- create table for backup info
CREATE TABLE IF NOT EXISTS relsizes_stats_schema.table_sizes_history
    (insert_date date NOT NULL, nspname text NOT NULL, relname text NOT NULL, size bigint NOT NULL, mtime timestamp NOT NULL)
    DISTRIBUTED BY (insert_date)
    PARTITION BY RANGE (insert_date)
    (
        PARTITION p1970_01_01 START ('1970-01-01') END ('1970-01-02') EXCLUSIVE
    );

-- create function that add actual partition
CREATE OR REPLACE FUNCTION create_actual_partition()
RETURNS void AS $$
DECLARE
    start_date date;
    end_date date;
    partition_exists boolean;
    partition_name text;
BEGIN
    -- Calculate the dynamic dates
    start_date := date_trunc('day', CURRENT_DATE);
    end_date := start_date + INTERVAL '1 day';
    partition_name := 'p' || to_char(start_date, 'YYYY_MM_DD');

    -- Check if the partition already exists
    SELECT EXISTS (
        SELECT 1
        FROM pg_partitions
        WHERE tablename = 'table_sizes_history'
          AND partitionname = partition_name
    ) INTO partition_exists;

    -- If the partition does not exist, add it
    IF NOT partition_exists THEN
        EXECUTE format(
            'ALTER TABLE relsizes_stats_schema.table_sizes_history ADD PARTITION %I START (''%s'') END (''%s'') EXCLUSIVE',
            partition_name,
            to_char(start_date, 'YYYY-MM-DD'),
            to_char(end_date, 'YYYY-MM-DD')
        );
        RAISE NOTICE 'Partition % created.', partition_name;
    ELSE
        RAISE NOTICE 'Partition % already exists.', partition_name;
    END IF;
END $$ LANGUAGE plpgsql;

-- create function that delete outdated partitions
CREATE OR REPLACE FUNCTION drop_old_partitions(interval_in_days INT) RETURNS VOID AS $$
DECLARE
    cutoff_date DATE := CURRENT_DATE - interval_in_days;  -- Calculate cutoff date based on input
    partition_record RECORD;
BEGIN
    -- Loop through the partitions that are older than the cutoff date
    FOR partition_record IN
        SELECT partitionname
        FROM pg_partitions
        WHERE tablename = 'table_sizes_history'
          AND partitionname ~ '^p[0-9]{4}_[0-9]{2}_[0-9]{2}$'
          AND to_date(substring(partitionname FROM 2 FOR 11), 'YYYY_MM_DD') < cutoff_date
    LOOP
        -- Construct and execute the ALTER TABLE DROP PARTITION query
        EXECUTE format('ALTER TABLE relsizes_stats_schema.table_sizes_history DROP PARTITION %I', partition_record.partitionname);
    END LOOP;
END $$ LANGUAGE plpgsql;



CREATE OR REPLACE VIEW relsizes_stats_schema.table_files AS
    WITH part_oids AS (
        SELECT n.nspname, c1.relname, c1.oid
        FROM pg_class c1
        JOIN pg_namespace n ON c1.relnamespace = n.oid
        WHERE c1.reltablespace != (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_global')
        UNION
        SELECT n.nspname, c1.relname, c2.oid
        FROM pg_class c1
        JOIN pg_namespace n ON c1.relnamespace = n.oid
        JOIN pg_partition pp ON c1.oid = pp.parrelid
        JOIN pg_partition_rule pr ON pp.oid = pr.paroid
        JOIN pg_class c2 ON pr.parchildrelid = c2.oid
        WHERE c1.reltablespace != (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_global')
    ),
    table_oids AS (
        SELECT po.nspname, po.relname, po.oid, 'main' AS kind
            FROM part_oids po
        UNION
        SELECT po.nspname, po.relname, t.reltoastrelid, 'toast' AS kind
            FROM part_oids po
            JOIN pg_class t ON po.oid = t.oid
            WHERE t.reltoastrelid > 0
        UNION
        SELECT po.nspname, po.relname, ti.indexrelid, 'toast_idx' AS kind
            FROM part_oids po
            JOIN pg_class t ON po.oid = t.oid
            JOIN pg_index ti ON t.reltoastrelid = ti.indrelid
            WHERE t.reltoastrelid > 0
        UNION
        SELECT po.nspname, po.relname, ao.segrelid, 'ao' AS kind
            FROM part_oids po
            JOIN pg_appendonly ao ON po.oid = ao.relid
        UNION
        SELECT po.nspname, po.relname, ao.visimaprelid, 'ao_vm' AS kind
            FROM part_oids po
            JOIN pg_appendonly ao ON po.oid = ao.relid
        UNION
        SELECT po.nspname, po.relname, ao.visimapidxid, 'ao_vm_idx' AS kind
            FROM part_oids po
            JOIN pg_appendonly ao ON po.oid = ao.relid
    )
    SELECT table_oids.nspname, table_oids.relname, m.segment, m.relfilenode, fs.filepath, kind, size, mtime
    FROM table_oids
    JOIN relsizes_stats_schema.segment_file_map m ON table_oids.oid = m.reloid
    JOIN relsizes_stats_schema.segment_file_sizes fs ON m.segment = fs.segment AND m.relfilenode = fs.relfilenode;
CREATE OR REPLACE VIEW relsizes_stats_schema.table_sizes AS
    SELECT nspname, relname, sum(size) AS size, to_timestamp(MAX(mtime)) AS mtime FROM relsizes_stats_schema.table_files
    GROUP BY nspname, relname;
CREATE OR REPLACE VIEW relsizes_stats_schema.namespace_sizes AS
    SELECT nspname, sum(size) AS size FROM relsizes_stats_schema.table_files
    GROUP BY nspname;
-- Here go any C or PL/SQL functions, table or view definitions etc
-- for example:

CREATE FUNCTION get_stats_for_database(dboid INTEGER)
RETURNS TABLE (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
AS 'MODULE_PATHNAME', 'get_stats_for_database'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

