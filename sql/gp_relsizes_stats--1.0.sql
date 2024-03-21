/* gp_relsizes_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relsizes_stats" to load this file. \quit

-- CREATE TABLE IF NOT EXISTS ... (....) DISTRIBUTED BY ... 

-- Here go any C or PL/SQL functions, table or view definitions etc
-- for example:
CREATE FUNCTION always_return_one()
RETURNS record
AS 
$$
  SELECT 1;
$$
LANGUAGE SQL IMMUTABLE EXECUTE ON MASTER;

CREATE FUNCTION always_return_square (in integer)
RETURNS integer
AS 'MODULE_PATHNAME', 'int_square_c_impl'
LANGUAGE C IMMUTABLE STRICT EXECUTE ON MASTER;

/*
CREATE FUNCTION get_list_of_databases ()
RETURNS TABLE (datname name, oid oid)
AS 'MODULE_PATHNAME', 'get_list_of_databases'
LANGUAGE C STRICT EXECUTE ON MASTER;
*/

CREATE FUNCTION gp_table_sizes()
RETURNS TABLE (datname name, oid oid)
AS 'MODULE_PATHNAME', 'gp_table_sizes'
LANGUAGE C STRICT EXECUTE ON MASTER;
