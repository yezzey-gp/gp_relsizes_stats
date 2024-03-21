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

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(int_square_c_impl);
PG_FUNCTION_INFO_V1(get_list_of_databases);
PG_FUNCTION_INFO_V1(gp_table_sizes);

void _PG_init(void);
void _PG_fini(void);

Datum 
int_square_c_impl(PG_FUNCTION_ARGS) {
  int64 in = DatumGetInt64(PG_GETARG_DATUM(0));
  PG_RETURN_INT64(in * in);
}

Datum 
get_list_of_databases(PG_FUNCTION_ARGS) {
    FILE *fptr = fopen("/tmp/shit", "a");
    fprintf(fptr, "start!!!\n");
    fclose(fptr);


    /* debug log about starting get_list_of_databases() function */
    elog(DEBUG1, "start collecting info about databases");
    fptr = fopen("/tmp/shit", "a");
    fprintf(fptr, "in function\n");
    fclose(fptr);

    /* create variables that needed in that function */
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    //Datum result;
    Tuplestorestate *tupstore;
    TupleDesc   tupdesc;
    SPITupleTable *spi_tuptable;
    TupleDesc   spi_tupdesc;
    char *sql = "SELECT datname, oid \
                 FROM pg_database \
                 WHERE datname NOT IN ('template0', 'template1', 'diskquota', 'gpperfmon')";
    int ret;
    int proc;


    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that cannot accept a set")));
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required, but it is not " \
                        "allowed in this context")));
    }

    /* Connect to SPI manager */
    if ((ret = SPI_connect()) < 0)
        /* internal error */
        elog(ERROR, "crosstab: SPI_connect returned %d", ret);

    /* Retrieve the desired rows */
    ret = SPI_execute(sql, true, 0);
    proc = SPI_processed;

    /* If no qualifying tuples, fall out early */
    if (ret != SPI_OK_SELECT || proc <= 0)
    {
        SPI_finish();
        rsinfo->isDone = ExprEndResult;
        PG_RETURN_NULL();
    }

    spi_tuptable = SPI_tuptable;
    spi_tupdesc = spi_tuptable->tupdesc;

    if (spi_tupdesc->natts != 2) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid source data SQL statement"),
                 errdetail("The provided SQL must return 3 "
                           "columns: rowid, category, and values.")));
    }

    /* get a tuple descriptor for our result type */
    switch(get_call_result_type(fcinfo, NULL, &tupdesc))
    {
        case TYPEFUNC_COMPOSITE:
            /* success */
            break;
        case TYPEFUNC_RECORD:
            /* failed to determine actual type of RECORD */
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));
            break;
        default:
            /* result type isn't composite */
            elog(ERROR, "return type must be a row type");
            break;
    }
    tupstore = tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random, false, work_mem);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    SPI_finish();

    return (Datum) 0;
    //PG_RETURN_DATUM(tupdesc);
}

Datum 
gp_table_sizes(PG_FUNCTION_ARGS) {
    elog(INFO, "start collecting info about tables for gp_table_sizes...");

    //ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    //TupleDesc   tupdesc;

    //tupdesc = get_list_of_databases();

    //rsinfo->returnMode = SFRM_Materialize;
    //rsinfo->setResult = ;
    //rsinfo->setDesc = tupdesc;

    return (Datum) 0;    
}

void _PG_init(void) {
  // nothing to do here for this template, but usually we register hooks here,
  // allocate shared memory, start background workers, etc
}

void _PG_fini(void) {
  // nothing to do here for this template
}
