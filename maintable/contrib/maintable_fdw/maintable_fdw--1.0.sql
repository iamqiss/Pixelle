/* contrib/maintable_fdw/maintable_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION maintable_fdw" to load this file. \quit

CREATE FUNCTION maintable_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION maintable_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER maintable_fdw
  HANDLER maintable_fdw_handler
  VALIDATOR maintable_fdw_validator;
