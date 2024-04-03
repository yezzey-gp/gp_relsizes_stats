/* gp_relsizes_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relsizes_stats" to load this file. \quit

-- CREATE TABLE IF NOT EXISTS ... (....) DISTRIBUTED BY ... 

-- Here go any C or PL/SQL functions, table or view definitions etc
-- for example:
CREATE FUNCTION get_file_sizes_for_database(dboid INTEGER)
RETURNS TABLE (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
AS 'MODULE_PATHNAME', 'get_file_sizes_for_database'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION collect_table_sizes(ignored_dbnames VARCHAR[])
RETURNS void
AS 'MODULE_PATHNAME', 'collect_table_sizes'
LANGUAGE C STRICT EXECUTE ON MASTER;
