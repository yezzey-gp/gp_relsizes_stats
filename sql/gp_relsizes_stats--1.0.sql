/* gp_relsizes_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relsizes_stats" to load this file. \quit


-- CREATE TABLE IF NOT EXISTS ... (....) DISTRIBUTED BY ... 
CREATE SCHEMA IF NOT EXISTS mdb_toolkit;

-- create table
CREATE TABLE IF NOT EXISTS mdb_toolkit.segment_file_map
    (segment INTEGER, reloid OID, relfilenode OID) 
    WITH (appendonly=true) DISTRIBUTED RANDOMLY;
-- create table 
CREATE TABLE IF NOT EXISTS mdb_toolkit.segment_file_sizes
    (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
    WITH (appendonly=true, OIDS=FALSE) DISTRIBUTED RANDOMLY;
TRUNCATE TABLE mdb_toolkit.segment_file_sizes;

CREATE OR REPLACE VIEW mdb_toolkit.table_files AS
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
    JOIN mdb_toolkit.segment_file_map m ON table_oids.oid = m.reloid
    JOIN mdb_toolkit.segment_file_sizes fs ON m.segment = fs.segment AND m.relfilenode = fs.relfilenode;
CREATE OR REPLACE VIEW mdb_toolkit.table_sizes AS
    SELECT nspname, relname, sum(size) AS size, to_timestamp(MAX(mtime)) AS mtime FROM mdb_toolkit.table_files
    GROUP BY nspname, relname;
CREATE OR REPLACE VIEW mdb_toolkit.namespace_sizes AS
    SELECT nspname, sum(size) AS size FROM mdb_toolkit.table_files
    GROUP BY nspname;
-- Here go any C or PL/SQL functions, table or view definitions etc
-- for example:

CREATE FUNCTION get_stats_for_database(dboid INTEGER)
RETURNS TABLE (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
AS 'MODULE_PATHNAME', 'get_stats_for_database'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

