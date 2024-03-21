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

PG_FUNCTION_INFO_V1(gp_table_sizes);
Datum
gp_table_sizes(PG_FUNCTION_ARGS) {
    /* default typed variables */
    int retcode, cnt_databases;
    elog(DEBUG1, "my debug");

    /* Postresql typed variables */
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                    errmsg("set-valued function called in context that cannot accept a set")));
        goto free_end;
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
                    errmsg("materialize mode required, but it is not allowed in this context")));
        goto free_end;
    }

    /* connect to SPI for sending queries to GP */
    retcode = SPI_connect();
    if (retcode < 0) {
        /* error */
        elog(ERROR, "gp_table_sizes: SPI_connect returned %d", retcode);
        goto free_end;
    } 
    elog(DEBUG1, "gp_table_sizes: connect to GP successfully");

    /* call get_list_of_databases() function to get 
     * list of pairs (datname, oid) for existing databases
     */
    retcode = get_list_of_databases(rsinfo);
    if (retcode < 0) {
        /* error */
        elog(ERROR, "gp_table_sizes: error -> get_list_of_databases: error during query execution");
        rsinfo->isDone = ExprEndResult;
        goto free_SPI;
    } else {
        /* set number of availible databases */
        cnt_databases = retcode;
    }
    elog(DEBUG1, "gp_table_sizes: get list of databases successfully");

    /* придумать как сохранить табличку и адекватно итерироваться по её строкам */

    /* CHECK DOES get_list_of_databases() was correctly executed; TODO: remove code in CHECK */
    int query_natts;
    bool randomAccess;
    TupleDesc query_tupdesc, result_tupdesc;
    Tuplestorestate* result_tupstore;
    SPITupleTable *query_tuptable;
    MemoryContext oldcontext;

    query_tuptable = SPI_tuptable;
    query_tupdesc = query_tuptable->tupdesc;
    query_natts = query_tupdesc->natts;

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    result_tupdesc = CreateTupleDescCopy(query_tupdesc);
    result_tupdesc = BlessTupleDesc(result_tupdesc);

    randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    result_tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = result_tupstore;
    rsinfo->setDesc = result_tupdesc;

    for (int i = 0; i < cnt_databases; ++i) {
        tuplestore_puttuple(result_tupstore, query_tuptable->vals[i]);
    }

    MemoryContextSwitchTo(oldcontext);
    /* CHECK FINISHED */

free_SPI:
    SPI_finish();
free_end:
    return (Datum) 0;
}

void _PG_init(void) {
  // nothing to do here for this template, but usually we register hooks here,
  // allocate shared memory, start background workers, etc
}

void _PG_fini(void) {
  // nothing to do here for this template
}
