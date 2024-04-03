/* gp_relsizes_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relsizes_stats" to load this file. \quit

-- CREATE TABLE IF NOT EXISTS ... (....) DISTRIBUTED BY ... 

-- Here go any C or PL/SQL functions, table or view definitions etc
-- for example:
CREATE FUNCTION prepare_info_tables() RETURNS VOID
LANGUAGE plpgsql VOLATILE EXECUTE ON MASTER AS
$func$
BEGIN
    -- create table, clear if it's exists and fill with actual data
    EXECUTE 'CREATE TABLE IF NOT EXISTS gp_toolkit.segment_file_map
        (segment INTEGER, reloid OID, relfilenode OID) 
        WITH (appendonly=true) DISTRIBUTED BY (segment)';
    EXECUTE 'TRUNCATE TABLE gp_toolkit.segment_file_map';
    EXECUTE 'INSERT INTO gp_toolkit.segment_file_map 
        SELECT gp_segment_id, oid, relfilenode FROM gp_dist_random(''pg_class'')';

    -- create table and clear if it's exists 
    EXECUTE 'CREATE TABLE IF NOT EXISTS gp_toolkit.segment_file_sizes
        (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
        WITH (appendonly=true, OIDS=FALSE) DISTRIBUTED RANDOMLY';
    EXECUTE 'TRUNCATE TABLE gp_toolkit.segment_file_sizes';
END
$func$;


CREATE FUNCTION get_file_sizes_for_database(dboid INTEGER)
RETURNS TABLE (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
AS 'MODULE_PATHNAME', 'get_file_sizes_for_database'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION collect_table_sizes(ignored_dbnames VARCHAR[])
RETURNS void
AS 'MODULE_PATHNAME', 'collect_table_sizes'
LANGUAGE C STRICT EXECUTE ON MASTER;

