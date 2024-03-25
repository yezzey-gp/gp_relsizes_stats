#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/trigger.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include <string.h>
#include <stdlib.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(int_square_c_impl);
PG_FUNCTION_INFO_V1(collect_table_size);

void _PG_init(void);
void _PG_fini(void);
static List* get_collectable_db_ids(List *ignored_db_names);
static int create_trunkate_table();
static void load_all_segments_stats(char *base_dir, int dboid, TimestampTz start_time);
Datum collect_table_size(PG_FUNCTION_ARGS);

static List* get_collectable_db_ids(List *ignored_db_names) {
    /* default C typed data */
    int retcode;
    char *sql = "SELECT datname, oid \
                 FROM pg_database \
                 WHERE datname NOT IN ('template0', 'template1', 'diskquota', 'gpperfmon')";

    /* PostgreSQL typed data */
    List *collectable_db_ids = NIL;

    /* connect to SPI */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        elog(ERROR, "gp_table_sizes: SPI_connect returned %d", retcode);
        goto finish_SPI;
    } 

    /* execute sql query to get table */
    retcode = SPI_execute(sql, true, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_SELECT || SPI_processed < 0) {
        elog(ERROR, "get_collectable_db_ids: SPI_execute returned %d, processed %lu rows", retcode, SPI_processed);
        goto finish_SPI;
    }

    Datum *tuple_values = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_values));
    bool *tuple_nullable = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_nullable));
    
    for (int i = 0; i < SPI_processed; ++i) {
        HeapTuple current_tuple = SPI_tuptable->vals[i];
        heap_deform_tuple(current_tuple, SPI_tuptable->tupdesc, tuple_values, tuple_nullable);
        
        /* check if datname not in ignored_db_names */
        bool ignored = false;
        ListCell *current_cell;

        foreach(current_cell, ignored_db_names) {
            retcode = strcmp((char *)lfirst(current_cell),
                    DatumGetCString(tuple_values[0]));
            if (retcode == 0) {
                ignored = true;
                break;
            }
        }

        if (!ignored) {
            collectable_db_ids = lappend(collectable_db_ids, (void *)tuple_values[1]);
        }
    }

    pfree(tuple_values);
    pfree(tuple_nullable);

finish_SPI:
    /* finish SPI */
    SPI_finish();
    return collectable_db_ids;
}

static int create_trunkate_table() {
    int retcode = 0;
    char *sql = "CREATE TABLE IF NOT EXISTS gp_collect_table_size(someArg varchar) DISTRIBUTED BY (someArg)"; // TODO

    /* connect to SPI */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        elog(ERROR, "create_trunkate_table: SPI_connect returned %d", retcode);
        retcode = -1;
        goto finish_SPI;
    }

    /* execute sql query to create table (if it not exists) */
    retcode = SPI_execute(sql, false, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_UTILITY) {
        elog(ERROR, "create_trunkate_table: SPI_execute returned %d (in create query)",
                retcode);
        retcode = -1;
        goto finish_SPI;
    }

    /* set new sql query */
    sql = "TRUNCATE TABLE gp_collect_table_size";

    /* execute new sql query to trunkate table */
    retcode = SPI_execute(sql, false, 0);
    
    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_UTILITY) {
        elog(ERROR, "create_trunkate_table: SPI_execute returned %d (in trunkate query)",
                retcode);
        retcode = -1;
        goto finish_SPI;
    }

finish_SPI:
    /* finish SPI */
    SPI_finish();
    return retcode;
}

static void load_all_segments_stats(char *base_dir, int dboid, TimestampTz start_time) {
    return;
}

static int write_collected_data(char *base_dir, int dboid) {
    return 0;
} 

Datum collect_table_size(PG_FUNCTION_ARGS) {
    /* default C typed data */
    bool elem_type_by_val;
    bool *args_nulls;
    char elem_alignment_code;
    char *base_dir;
    int16 elem_width;
    int args_count, retcode, dboid;

    /* PostreSQL typed data */
    ArrayType *ignored_db_names_array;
    Datum  *args_datums;
    List *ignored_db_names = NIL, *databases_ids;
    ListCell *current_cell;
    Oid elem_type;
    TimestampTz collecting_start_time;

    // put all ignored_db names from fisrt array-argument
    ignored_db_names_array = PG_GETARG_ARRAYTYPE_P(0);
    elem_type = ARR_ELEMTYPE(ignored_db_names_array);
    get_typlenbyvalalign(elem_type, &elem_width, &elem_type_by_val, &elem_alignment_code);
    deconstruct_array(ignored_db_names_array, elem_type, elem_width, elem_type_by_val, elem_alignment_code, &args_datums, &args_nulls, &args_count);
    for (int i = 0; i < args_count; ++i) {
        ignored_db_names = lappend(ignored_db_names, (void *)DatumGetCString(DirectFunctionCall1(textout, args_datums[i])));
    }

    // get base_dir for csv storing
    base_dir = DatumGetCString(DirectFunctionCall1(textout, (Datum)PG_GETARG_TEXT_P(1)));

    databases_ids = get_collectable_db_ids(ignored_db_names);
    
    retcode = create_trunkate_table(); 
    if (retcode < 0) {
        elog(ERROR, "collect_table_size: create_trunkate_table: can not create or trunkate table");
        PG_RETURN_VOID();
    }
    
    foreach(current_cell, databases_ids) {
        dboid = lfirst_int(current_cell); 
        collecting_start_time = GetCurrentTimestamp();
        load_all_segments_stats(base_dir, dboid, collecting_start_time);

        retcode = write_collected_data(base_dir, dboid);
        if (retcode < 0) {
            elog(ERROR, "collect_table_size: write_collected_data: can not write data from csv to table");
            break;
        }
    }

    PG_RETURN_VOID();
}

void _PG_init(void) {
    // nothing to do here for this template, but usually we register hooks here,
    // allocate shared memory, start background workers, etc
}

void _PG_fini(void) {
  // nothing to do here for this template
}
