#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
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
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include <limits.h> 
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>

#define MAX_QUERY_SIZE 10000 // TODO

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(collect_table_size);
PG_FUNCTION_INFO_V1(get_file_sizes_for_database);

void _PG_init(void);
void _PG_fini(void);
static List* get_collectable_db_ids(List *ignored_db_names, MemoryContext saved_context);
static int create_truncate_fill_tables();
static bool is_number(char symbol);
static unsigned int fill_relfilenode(char *name);
static void fill_file_sizes(int segment_id, char *data_dir, char *csv_path, MemoryContext saved_context, ReturnSetInfo* rsinfo, FunctionCallInfo fcinfo);
static int get_file_sizes_for_databases(List *databases_ids, char *dest_dir);

Datum get_file_sizes_for_database(PG_FUNCTION_ARGS);
Datum collect_table_size(PG_FUNCTION_ARGS);

static List* get_collectable_db_ids(List *ignored_db_names, MemoryContext saved_context) {
    /* default C typed data */
    int retcode;
    char *sql = "SELECT datname, oid \
                 FROM pg_database \
                 WHERE datname NOT IN ('template0', 'template1', 'diskquota', 'gpperfmon')";

    /* PostgreSQL typed data */
    MemoryContext old_context = MemoryContextSwitchTo(saved_context);
    List *collectable_db_ids = NIL;
    MemoryContextSwitchTo(old_context);

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
            MemoryContext old_context = MemoryContextSwitchTo(saved_context);
            collectable_db_ids = lappend_int(collectable_db_ids, DatumGetInt32(tuple_values[1]));
            MemoryContextSwitchTo(old_context);
        }
    }

    pfree(tuple_values);
    pfree(tuple_nullable);

finish_SPI:
    /* finish SPI */
    SPI_finish();
    return collectable_db_ids;
}

static int create_truncate_fill_tables() {
    int retcode = 0;

    /* CREATE, TRUNCATE AND FILL TABLE WITH MAPPINGS */

    char *sql = "CREATE TABLE IF NOT EXISTS gp_toolkit.segment_file_map \
        ( \
           segment       int, \
           reloid        oid, \
           relfilenode   oid \
        ) \
        WITH (appendonly=true) \
        DISTRIBUTED BY (segment)";

    /* connect to SPI */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        elog(ERROR, "create_truncate_fill_tables: SPI_connect returned %d", retcode);
        retcode = -1;
        goto finish_SPI;
    }

    /* execute sql query to create table (if it not exists) */
    retcode = SPI_execute(sql, false, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_UTILITY) {
        elog(ERROR, "create_truncate_fill_tables: SPI_execute returned %d (in create query)", retcode);
        retcode = -1;
        goto finish_SPI;
    }

    /* set new sql query */
    sql = "TRUNCATE TABLE gp_toolkit.segment_file_map";

    /* execute new sql query to trunkate table */
    retcode = SPI_execute(sql, false, 0);
    
    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_UTILITY) {
        elog(ERROR, "create_truncate_fill_tables: SPI_execute returned %d (in truncate query)",
                retcode);
        retcode = -1;
        goto finish_SPI;
    }

    sql = "INSERT INTO gp_toolkit.segment_file_map \
        SELECT gp_segment_id, oid, relfilenode FROM gp_dist_random('pg_class')";

    /* execute sql query to insert values into new table */
    retcode = SPI_execute(sql, false, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_INSERT) {
        elog(ERROR, "create_truncate_fill_tables: SPI_execute returned %d (in insert query)", retcode);
        retcode = -1;
        goto finish_SPI;
    }

    /* CREATE AND TRUNCATE TABLE WITH FILE SIZES */

    sql = "CREATE TABLE IF NOT EXISTS gp_toolkit.segment_file_sizes \
        ( \
           segment       int, \
           relfilenode   oid, \
           filepath      text, \
           size          bigint, \
           mtime         bigint \
        ) \
        WITH (appendonly=true, OIDS=FALSE) \
        DISTRIBUTED RANDOMLY";

    /* execute sql query to create table */
    retcode = SPI_execute(sql, false, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_UTILITY) {
        elog(ERROR, "create_truncate_fill_tables: SPI_execute returned %d (in create query)", retcode);
        retcode = -1;
        goto finish_SPI;
    }

    sql = "TRUNCATE TABLE gp_toolkit.segment_file_sizes";

    /* execute sql query to create table */
    retcode = SPI_execute(sql, false, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_UTILITY) {
        elog(ERROR, "create_truncate_fill_tables: SPI_execute returned %d (in truncate query)", retcode);
        retcode = -1;
        goto finish_SPI;
    }

finish_SPI:
    /* finish SPI */
    SPI_finish();
    return retcode;
}

static bool is_number(char symbol) {
    return '0' <= symbol && symbol <= '9';
}

static unsigned int fill_relfilenode(char *name) {
    char dst[PATH_MAX];
    memset(dst, 0, PATH_MAX);
    int start_pos = 0, pos = 0;
    while (start_pos < strlen(name) && !is_number(name[start_pos])) {
        ++start_pos;
    }
    while (start_pos < strlen(name) && is_number(name[start_pos])) {
        dst[pos++] = name[start_pos++];
    }
    return strtoul(dst, NULL, 10);
}

static void fill_file_sizes(int segment_id, char *data_dir, char *csv_path, MemoryContext saved_context, ReturnSetInfo* rsinfo, FunctionCallInfo fcinfo) {
    FILE *fptr = fopen("/tmp/shit", "a");
    fprintf(fptr, "at the begin %d with data_dir %s\n", segment_id, data_dir);
    fclose(fptr);
    /* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
    MemoryContext oldcontext = MemoryContextSwitchTo(saved_context);

    /* Makes the output TupleDesc */
    TupleDesc tupdesc;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");
    tupdesc = BlessTupleDesc(tupdesc);

    /* Checks if random access is allowed */
    bool randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    /* Starts the tuplestore */
    Tuplestorestate* tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);

    /* Set the output */
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    Datum outputValues[5];
    bool outputNulls[5];
    MemSet(outputNulls, 0, sizeof(outputNulls));

    /* Returns to the old context */
    MemoryContextSwitchTo(oldcontext);


    /* if {path} is NULL => return */
    if (!data_dir) {
        return;
    }

    char new_path[PATH_MAX];
    char relfilenode[PATH_MAX];
    memset(relfilenode, 0, PATH_MAX);

    DIR *current_dir = AllocateDir(data_dir);
    /* if {current_dir} did not opened => return */
    if (!current_dir) {
        return;
    }

    struct dirent *file;
    /* start itterating in {current_dir} */
    while ((file = ReadDir(current_dir, data_dir)) != NULL) {
        char *filename = file->d_name;

        /* if filename is special as "." or ".." => continue */
        if (strcmp(filename, ".") == 0 || strcmp(filename, "..") == 0) {
            continue;
        }

        /* if limit of PATH_MAX reached skip file */
        if (sprintf(new_path, "%s/%s", data_dir, filename) >= sizeof(new_path)) {
            continue;
        }

        struct stat stb;
        /* do lstat if returned error => continue */
        if (lstat(new_path, &stb) < 0) {
            continue;
        }

        if (S_ISREG(stb.st_mode)) {      
            // here write to csv data about file
            // need to write ({start_time}, {segment}, {filename_numbers}, {filename}, {stb.st_size}, {stb.st_mtimespec})
            // can store a lot of different stats here (lstat sys call is MVP (P = player))
            outputValues[0] = Int32GetDatum(segment_id);
            outputValues[1] = ObjectIdGetDatum(fill_relfilenode(filename));
            outputValues[2] = CStringGetTextDatum(filename);
            outputValues[3] = Int64GetDatum(stb.st_size);
            outputValues[4] = Int64GetDatum(stb.st_mtime);
            
            /* Builds the output tuple (row) */
            /* Puts in the output tuplestore */
            tuplestore_putvalues(tupstore, tupdesc, outputValues, outputNulls);
        } else if (S_ISDIR(stb.st_mode)) {
            fill_file_sizes(segment_id, new_path, csv_path, saved_context, rsinfo, fcinfo);
        }
    }
    FreeDir(current_dir);

    fptr = fopen("/tmp/shit", "a");
    fprintf(fptr, "at the end %d with data_dir %s\n", segment_id, data_dir);
    fclose(fptr);
}

Datum get_file_sizes_for_database(PG_FUNCTION_ARGS) {
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("materialize mode required, but it is not allowed in this context")));


    int segment_id = GpIdentity.segindex;
    int dboid = PG_GETARG_INT32(0);

    /* {dest_dir}/{dboid}/files_gpseg{segment_id}.csv - in Ditriy's code, 
     * but we store that csv on segment => we can use that way
     * {dest_dir}/files_gp{dboid}.csv => no need to create directory for each db
     */
    
    char cwd[PATH_MAX];
    char csv_path[PATH_MAX];
    char data_dir[PATH_MAX];
    getcwd(cwd, sizeof(cwd));

    char dest_dir[PATH_MAX];
    memset(dest_dir, 0, PATH_MAX);
    sprintf(dest_dir, "%s/dir", cwd);
    struct stat st = {0};
    if (stat(dest_dir, &st) == -1) {
        mkdir(dest_dir, 0770);
    }
    
    sprintf(csv_path, "%s/files_gp%d_%d.csv", dest_dir, segment_id, dboid);
    sprintf(data_dir, "%s/base/%d", cwd, dboid);

    /* clear file (maybe use unlink here)*/
    truncate(csv_path, 0);

    
    // i need segment_id, dboid, base_dir (where files placed), csv_path
    fill_file_sizes(segment_id, data_dir, csv_path, CurrentMemoryContext, rsinfo, fcinfo);

    FILE *fptr = fopen("/tmp/shit", "a");
    fprintf(fptr, "at the final end %d\n", segment_id);
    fclose(fptr);

    return (Datum) 0;
}

static int get_file_sizes_for_databases(List *databases_ids, char *dest_dir) {
    /* default C typed data */
    int retcode = 0;
    char query[MAX_QUERY_SIZE];
    
    /* PostreSQL typed data */
    ListCell *current_cell;

    foreach(current_cell, databases_ids) {
        int dbid = lfirst_int(current_cell); 
        sprintf(query, "INSERT INTO gp_toolkit.segment_file_sizes (segment, relfilenode, filepath, size, mtime) \ 
                SELECT get_file_sizes_for_database(%d)", dbid);

        FILE *fptr = fopen("/tmp/shit", "a");
        fprintf(fptr, "%s\n", query);
        fclose(fptr);

        /* connect to SPI */
        retcode = SPI_connect();
        if (retcode < 0) { /* error */
            elog(ERROR, "get_file_sizes_for_databases: SPI_connect returned %d", retcode);
            retcode = -1;
            goto finish_SPI;
        }

        /* execute sql query to create table (if it not exists) */
        retcode = SPI_execute(query, false, 0);

        /* check errors if they're occured during execution */
        if (retcode != SPI_OK_INSERT) {
            elog(ERROR, "get_file_sizes_for_databases: SPI_execute returned %d", retcode);
            retcode = -1;
            goto finish_SPI;
        }

finish_SPI:
        SPI_finish();
        if (retcode < 0) {
            break;
        }
    }

    return retcode;
}

static int write_final_result() {
    int retcode = 0;
    char *sql = "CREATE OR REPLACE VIEW gp_toolkit.table_files AS \
        WITH part_oids AS ( \
            SELECT n.nspname, c1.relname, c1.oid \
            FROM pg_class c1 \
            JOIN pg_namespace n ON c1.relnamespace = n.oid \
            WHERE c1.reltablespace != (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_global') \
            UNION \
            SELECT n.nspname, c1.relname, c2.oid \
            FROM pg_class c1 \
            JOIN pg_namespace n ON c1.relnamespace = n.oid \
            JOIN pg_partition pp ON c1.oid = pp.parrelid \
            JOIN pg_partition_rule pr ON pp.oid = pr.paroid \
            JOIN pg_class c2 ON pr.parchildrelid = c2.oid \
            WHERE c1.reltablespace != (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_global') \
        ), \
        table_oids AS ( \
            SELECT po.nspname, po.relname, po.oid, 'main' AS kind \
                FROM part_oids po \
            UNION \
            SELECT po.nspname, po.relname, t.reltoastrelid, 'toast' AS kind \
                FROM part_oids po \
                JOIN pg_class t ON po.oid = t.oid \
                WHERE t.reltoastrelid > 0 \
            UNION \
            SELECT po.nspname, po.relname, ti.indexrelid, 'toast_idx' AS kind \
                FROM part_oids po \
                JOIN pg_class t ON po.oid = t.oid \
                JOIN pg_index ti ON t.reltoastrelid = ti.indrelid \
                WHERE t.reltoastrelid > 0 \
            UNION \
            SELECT po.nspname, po.relname, ao.segrelid, 'ao' AS kind \
                FROM part_oids po \
                JOIN pg_appendonly ao ON po.oid = ao.relid \
            UNION \
            SELECT po.nspname, po.relname, ao.visimaprelid, 'ao_vm' AS kind \
                FROM part_oids po \
                JOIN pg_appendonly ao ON po.oid = ao.relid \
            UNION \
            SELECT po.nspname, po.relname, ao.visimapidxid, 'ao_vm_idx' AS kind \
                FROM part_oids po \
                JOIN pg_appendonly ao ON po.oid = ao.relid \
        ) \
        SELECT table_oids.nspname, table_oids.relname, m.segment, m.relfilenode, fs.filepath, kind, size, mtime \
        FROM table_oids \
        JOIN gp_toolkit.segment_file_map m ON table_oids.oid = m.reloid \
        JOIN gp_toolkit.segment_file_sizes fs ON m.segment = fs.segment AND m.relfilenode = fs.relfilenode";

    /* connect to SPI */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        elog(ERROR, "TODO: SPI_connect returned %d", retcode); // TODO
        retcode = -1;
        goto finish_SPI;
    }

    /* execute sql query to create view (if it not exists) */
    retcode = SPI_execute(sql, false, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_UTILITY) {
        elog(ERROR, "TODO: SPI_execute returned %d (in create query)", retcode); //TODO
        retcode = -1;
        goto finish_SPI;
    }

    sql = "CREATE OR REPLACE VIEW gp_toolkit.table_sizes AS \
        SELECT nspname, relname, sum(size) AS size, to_timestamp(MAX(mtime)) AS mtime FROM gp_toolkit.table_files \
        GROUP BY nspname, relname";

    /* execute sql query to create view (if it not exists) */
    retcode = SPI_execute(sql, false, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_UTILITY) {
        elog(ERROR, "TODO: SPI_execute returned %d (in create query)", retcode); //TODO
        retcode = -1;
        goto finish_SPI;
    }

    sql = "CREATE OR REPLACE VIEW gp_toolkit.namespace_sizes AS \
        SELECT nspname, sum(size) AS size FROM gp_toolkit.table_files \
        GROUP BY nspname";

    /* execute sql query to create view (if it not exists) */
    retcode = SPI_execute(sql, false, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_UTILITY) {
        elog(ERROR, "TODO: SPI_execute returned %d (in create query)", retcode); //TODO
        retcode = -1;
        goto finish_SPI;
    }

finish_SPI:
    SPI_finish();
    return retcode; 
}

Datum collect_table_size(PG_FUNCTION_ARGS) {
    /* default C typed data */
    bool elem_type_by_val;
    bool *args_nulls;
    char elem_alignment_code;
    char *dest_dir;
    int16 elem_width;
    int args_count;

    /* PostreSQL typed data */
    ArrayType *ignored_db_names_array;
    Datum  *args_datums;
    List *ignored_db_names = NIL, *databases_ids;
    Oid elem_type;
    MemoryContext saved_context = CurrentMemoryContext;

    // put all ignored_db names from fisrt array-argument
    ignored_db_names_array = PG_GETARG_ARRAYTYPE_P(0);
    elem_type = ARR_ELEMTYPE(ignored_db_names_array);
    get_typlenbyvalalign(elem_type, &elem_width, &elem_type_by_val, &elem_alignment_code);
    deconstruct_array(ignored_db_names_array, elem_type, elem_width, elem_type_by_val, elem_alignment_code, &args_datums, &args_nulls, &args_count);
    for (int i = 0; i < args_count; ++i) {
        ignored_db_names = lappend(ignored_db_names, (void *)DatumGetCString(DirectFunctionCall1(textout, args_datums[i])));
    }

    // get base_dir for csv storing
    dest_dir = DatumGetCString(DirectFunctionCall1(textout, (Datum)PG_GETARG_TEXT_P(1)));

    databases_ids = get_collectable_db_ids(ignored_db_names, saved_context);
    
    if (create_truncate_fill_tables() < 0) {
        PG_RETURN_VOID();
    }

    if (get_file_sizes_for_databases(databases_ids, dest_dir) < 0) {
        PG_RETURN_VOID();
    }

    // join tables and insert into final
    if (write_final_result() < 0) {
        PG_RETURN_VOID();
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
