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

PG_FUNCTION_INFO_V1(int_square_c_impl);
PG_FUNCTION_INFO_V1(gp_table_sizes);
//PG_FUNCTION_INFO_V1(get_list_of_databases);

void _PG_init(void);
void _PG_fini(void);
Datum* get_list_of_databases(ReturnSetInfo *rsinfo);

Datum 
int_square_c_impl(PG_FUNCTION_ARGS) {
  int64 in = DatumGetInt64(PG_GETARG_DATUM(0));
  PG_RETURN_INT64(in * in);
}

Datum*
get_list_of_databases(ReturnSetInfo *rsinfo) {
    //bool randomAccess;
    int retcode, query_rows, query_natts;
    char *sql = "SELECT datname, oid \
                 FROM pg_database \
                 WHERE datname NOT IN ('template0', 'template1', 'diskquota', 'gpperfmon')";

	//ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc query_tupdesc;//, result_tupdesc;
    //Tuplestorestate* result_tupstore;
    SPITupleTable *query_tuptable;
    MemoryContext oldcontext;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("set-valued function called in context that cannot accept a set")));
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("materialize mode required, but it is not allowed in this context")));
    }

    /* Connect to SPI manager */
    retcode = SPI_connect();
    if (retcode < 0) {
        elog(ERROR, "get_list_of_databases: SPI_connect returned %d", retcode);
    } 

    /* execute sql query to get table */
    retcode = SPI_execute(sql, true, 0);
    query_rows = SPI_processed;

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_SELECT || query_rows < 0) {
        SPI_finish();
        rsinfo->isDone = ExprEndResult; // some magic line ??? TODO: get info about it
        return (Datum) 0;
    }

    /* get pointers to taptable and tupdesc */
	query_tuptable = SPI_tuptable;
	query_tupdesc = query_tuptable->tupdesc;
    query_natts = query_tupdesc->natts;

    /* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    //result_tupdesc = CreateTupleDescCopy(query_tupdesc);
    //result_tupdesc = BlessTupleDesc(result_tupdesc);

    /* Checks if random access is allowed */
    //randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    //result_tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
    Datum* databases_table = palloc0(query_rows * sizeof(*databases_table));
    //!!!databases_table[0] = HeapTupleGetDatum(query_tupdesc);
    for (int i = 0; i < query_rows; ++i) {
        databases_table[i] = HeapTupleGetDatum(query_tuptable->vals[i]);
    }
    /* Set the output */
    //rsinfo->returnMode = SFRM_Materialize;
    //rsinfo->setResult = result_tupstore;
    //rsinfo->setDesc = result_tupdesc;

    /* Returns to the old context */
    MemoryContextSwitchTo(oldcontext);

    /*
    for (int i = 0; i < query_rows; ++i) {
        tuplestore_puttuple(result_tupstore, query_tuptable->vals[i]);
    }
    */

    SPI_finish();

    

    return databases_table;
}

Datum 
gp_table_sizes(PG_FUNCTION_ARGS) {
    elog(INFO, "start collecting info about tables for gp_table_sizes...");

	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    Datum* databases_table = get_list_of_databases(rsinfo);

    bool randomAccess;
    TupleDesc result_tupdesc;
    Tuplestorestate* result_tupstore;


    result_tupdesc = lookup_rowtype_tupdesc(
            HeapTupleHeaderGetTypeId(databases_table[0]), 
            HeapTupleHeaderGetTypMod(databases_table[1]));
    result_tupdesc = BlessTupleDesc(result_tupdesc);

    randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    result_tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);

    /* Set the output */
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = result_tupstore;
    rsinfo->setDesc = result_tupdesc;

    for (int i = 0; i < sizeof(databases_table); ++i) {
        HeapTuple current_tuple = DatumGetHeapTupleHeader(databases_table[i]);
        tuplestore_puttuple(result_tupstore, current_tuple);

        if (i == 0) {
            Oid tupType = HeapTupleHeaderGetTypeId(tup_hdr);
            int32_t tupTypmod = HeapTupleHeaderGetTypMod(tup_hdr);
            result_tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
        }
    }

    return (Datum) 0;    
}

void _PG_init(void) {
  // nothing to do here for this template, but usually we register hooks here,
  // allocate shared memory, start background workers, etc
}

void _PG_fini(void) {
  // nothing to do here for this template
}
