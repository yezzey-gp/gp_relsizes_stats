#include "postgres.h"

#include "access/xact.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "access/htup_details.h"

PG_MODULE_MAGIC;

int get_list_of_databases(ReturnSetInfo *rsinfo);
Datum gp_table_sizes(PG_FUNCTION_ARGS);
void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(int_square_c_impl);
Datum 
int_square_c_impl(PG_FUNCTION_ARGS) {
  int64 in = DatumGetInt64(PG_GETARG_DATUM(0));
  PG_RETURN_INT64(in * in);
}

int
get_list_of_databases(ReturnSetInfo *rsinfo) {
    int retcode, query_rows;
    char *sql = "SELECT datname, oid \
                 FROM pg_database \
                 WHERE datname NOT IN ('template0', 'template1', 'diskquota', 'gpperfmon')";

    /* execute sql query to get table */
    retcode = SPI_execute(sql, true, 0);
    query_rows = SPI_processed;

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_SELECT || query_rows < 0) {
        return -1;
    }

    return query_rows;
}

/* save (rows_cnt) rows from column with {column_number}
 * to {column_data} array, and save is_nullable option
 * in {*column_nullable} variable (get data from SPI_tuptable)
 *
 * all arguents should be correct 
 * -> column with {column_number} exists
 * -> {rows_cnt} rows exist 
 * -> sizeof({column_data}) >= rows_cnt
 * -> sizeof({column_nullable}) >= 1
 *
 * - creates own MemoryContext under {column_data}'s context
 * - delete own MemoryContext if error occured or successfully finished
 *  
 * return 0 - if finished successfully
 * return -1 - if finished unsuccessfully (error happens || input data incorrect) 
 */
int
store_table_column(int column_number, int rows_cnt, Datum* column_data, bool* column_nullable) { 
    int retcode = 0;

    /* check that all arguments has available values */
    if (column_number >= SPI_tuptable->tupdesc->natts ||
            rows_cnt > SPI_processed ||
            rows_cnt > sizeof(column_data) ||
            1 > sizeof(column_nullable)) {
        retcode = -1;
        goto MemoryContextDeletion;
    }

    /* create per function  MemoryContext */
    MemoryContext parent_context, old_context, function_context;
    parent_context = GetMemoryChunkContext(column_data);
    function_context = AllocSetContextCreate(parent_context, "store_table_column_context", ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(function_context);

    /* allocate memory for row storing */
    Datum* current_values = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*current_values));
    if (current_values == NULL) {
        elog(ERROR, "store_table_column: can not allocate memory for {current_values}");
        retcode = -1;
        goto MemoryContextDeletion;
    }

    /* allocate memory for row_nullable info storing */
    bool* nullable_values = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*nullable_values));
    if (nullable_values == NULL) {
        elog(ERROR, "store_table_column: can not allocate memory for {nullable_values}");
        retcode = -1;
        goto MemoryContextDeletion;
    }

    /* read and save data from table */
    for (int i = 0; i < rows_cnt; ++i) {
        HeapTuple current_tuple = SPI_tuptable->vals[i]; 
        heap_deform_tuple(current_tuple, SPI_tuptable->tupdesc, current_values, nullable_values);

        column_data[i] = current_values[column_number];
        *datnames_is_nullable = nullable_values[column_number];
    }

MemoryContextDeletion:
    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(function_context);
    return retcode;
}

PG_FUNCTION_INFO_V1(gp_table_sizes);
Datum
gp_table_sizes(PG_FUNCTION_ARGS) {
    /* default typed variables */
    int retcode, databases_cnt, query_natts;

    /* Postresql typed variables */
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    MemoryContext old_context, per_query_context;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                    errmsg("set-valued function called in context that cannot accept a set")));
        goto return_GP;
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
                    errmsg("materialize mode required, but it is not allowed in this context")));
        goto return_GP;
    }

    /* srote per_query_memory context */
    per_query_context = rsinfo->econtext->ecxt_per_query_memory;

    /* connect to SPI for sending queries to GP */
    retcode = SPI_connect();
    if (retcode < 0) {
        /* error */
        elog(ERROR, "gp_table_sizes: SPI_connect returned %d", retcode);
        goto return_GP;
    } 
    elog(DEBUG1, "gp_table_sizes: connect to GP successfully");

    /* call get_list_of_databases() function to get 
     * list of pairs (datname, oid) for existing databases
     */
    retcode = get_list_of_databases(rsinfo);
    if (retcode < 0) {
        /* error */
        elog(ERROR, "gp_table_sizes: error in get_list_of_databases: error during query execution");
        rsinfo->isDone = ExprEndResult;
        goto finish_SPI;
    } else {
        /* set number of availible databases */
        databases_cnt = retcode;
    }
    elog(DEBUG1, "gp_table_sizes: get list of databases successfully");

    /* придумать как сохранить табличку и адекватно итерироваться по её строкам */

    old_context = MemoryContextSwitchTo(per_query_context);
    /* allocate list for databases names */
    Datum* datnames = palloc0(databases_cnt * sizeof(*datnames));
    bool datnames_is_nullable = false;
    /* allocate list for databases oid */
    Datum* databases_oid = palloc0(databases_cnt * sizeof(*databases_oid));
    bool databases_oid_is_nullable = false;
    MemoryContextSwitchTo(old_context);

    if (store_table_column(0, databases_cnt, datnames, &datnames_is_nullable) < 0) {
        elog(ERROR, "gp_table_sizes: error in store_table_column for datnames");
        goto finish_SPI;
    }
    if (store_table_column(1, databases_cnt, databases_oid, &databases_oid_is_nullable) < 0) {
        elog(ERROR, "gp_table_sizes: error in store_table_column for databases_oid");
        goto finish_SPI;
    }

    /* collect info about each database */
    for (int i = 0; i < databases_cnt; ++i) {

    }

finish_SPI:
    SPI_finish();
return_GP:
    return (Datum) 0;
}

void _PG_init(void) {
  // nothing to do here for this template, but usually we register hooks here,
  // allocate shared memory, start background workers, etc
}

void _PG_fini(void) {
  // nothing to do here for this template
}

