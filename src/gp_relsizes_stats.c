#include "postgres.h"

#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include <sys/stat.h>

#define MAX_QUERY_SIZE PATH_MAX // obviously 150 is enough (150 > 135 + 10)

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(collect_table_sizes);
PG_FUNCTION_INFO_V1(get_file_sizes_for_database);

void _PG_init(void);
void _PG_fini(void);
static List *get_collectable_db_ids(List *ignored_db_names, FunctionCallInfo fcinfo);
static int create_truncate_fill_tables();
static bool is_number(char symbol);
static unsigned int fill_relfilenode(char *name);
static void fill_file_sizes(int segment_id, char *data_dir, FunctionCallInfo fcinfo);
static int get_file_sizes_for_databases(List *databases_ids);

Datum get_file_sizes_for_database(PG_FUNCTION_ARGS);
Datum collect_table_sizes(PG_FUNCTION_ARGS);

static List *get_collectable_db_ids(List *ignored_db_names, FunctionCallInfo fcinfo) {
    int retcode;
    char *sql = "SELECT datname, oid \
                 FROM pg_database \
                 WHERE datname NOT IN ('template0', 'template1', 'diskquota', 'gpperfmon')";
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

    /* connect to SPI */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        ereport(ERROR, (errmsg("gp_table_sizes: SPI_connect failed")));
    }

    /* execute sql query to get table */
    retcode = SPI_execute(sql, true, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_SELECT || SPI_processed < 0) { /* error */
        SPI_finish();
        ereport(ERROR, (errmsg("get_collectable_db_ids: SPI_execute failed (select datname, oid)")));
    }

    /* result store arrays */
    MemoryContext old_context = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    List *collectable_db_ids = NIL;
    MemoryContextSwitchTo(old_context);

    Datum *tuple_values = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_values));
    bool *tuple_nullable = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_nullable));

    for (int i = 0; i < SPI_processed; ++i) {
        HeapTuple current_tuple = SPI_tuptable->vals[i];
        heap_deform_tuple(current_tuple, SPI_tuptable->tupdesc, tuple_values, tuple_nullable);

        /* check if datname not in ignored_db_names */
        bool ignored = false;
        ListCell *current_cell;

        foreach (current_cell, ignored_db_names) {
            retcode = strcmp((char *)lfirst(current_cell), DatumGetCString(tuple_values[0]));
            if (retcode == 0) {
                ignored = true;
                break;
            }
        }

        if (!ignored) {
            MemoryContext old_context = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
            collectable_db_ids = lappend_int(collectable_db_ids, DatumGetInt32(tuple_values[1]));
            MemoryContextSwitchTo(old_context);
        }
    }

    pfree(tuple_values);
    pfree(tuple_nullable);

    SPI_finish();
    return collectable_db_ids;
}

static bool is_number(char symbol) { return '0' <= symbol && symbol <= '9'; }

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

static void fill_file_sizes(int segment_id, char *data_dir, FunctionCallInfo fcinfo) {
    /* if {path} is NULL => return */
    if (!data_dir) {
        return;
    }

    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    /* Check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("set-valued function called in context that cannot "
                                                                "accept a set")));
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("materialize mode required, but it is not allowed "
                                                              "in this context")));
    }

    /* Switch to query context */
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    /* Make the output TupleDesc */
    TupleDesc tupdesc;
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR, (errmsg("fill_file_sizes: incorrect return type in fcinfo (must be a row type)")));
    }
    tupdesc = BlessTupleDesc(tupdesc);

    /* Checks if random access is allowed */
    bool randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    /* Starts the tuplestore */
    Tuplestorestate *tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);

    /* Set the output */
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    Datum outputValues[5];
    bool outputNulls[5];
    MemSet(outputNulls, 0, sizeof(outputNulls));

    /* Returns to the old context */
    MemoryContextSwitchTo(oldcontext);

    char new_path[PATH_MAX];
    char relfilenode[PATH_MAX];
    memset(relfilenode, 0, PATH_MAX);

    DIR *current_dir = AllocateDir(data_dir);
    /* if {current_dir} did not opened => return */
    if (!current_dir) {
        ereport(ERROR, (errmsg("fill_file_sizes: failed to allocate current directory")));
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
            /* If file is regular we should count 
             * its size and put values into tupstore
             *
             * insert tuple:
             * (segment_is, relfilenode, new_path, stb.st_size, stb.st_mtime)
             */
            outputValues[0] = Int32GetDatum(segment_id);
            outputValues[1] = ObjectIdGetDatum(fill_relfilenode(filename));
            outputValues[2] = CStringGetTextDatum(new_path);
            outputValues[3] = Int64GetDatum(stb.st_size);
            outputValues[4] = Int64GetDatum(stb.st_mtime);

            /* Builds the output tuple (row)
             * and put it in the tuplestore 
             */
            tuplestore_putvalues(tupstore, tupdesc, outputValues, outputNulls);
        } else if (S_ISDIR(stb.st_mode)) {
            fill_file_sizes(segment_id, new_path, fcinfo);
        }
    }
    FreeDir(current_dir);
}

Datum get_file_sizes_for_database(PG_FUNCTION_ARGS) {
    char cwd[PATH_MAX];
    char data_dir[PATH_MAX];

    int segment_id = GpIdentity.segindex;
    int dboid = PG_GETARG_INT32(0);

    getcwd(cwd, sizeof(cwd));
    if (sprintf(data_dir, "%s/base/%d", cwd, dboid) >= sizeof(data_dir)) {
        ereport(ERROR, (errmsg("get_file_sizes_for_database: failed to write path to data_dir (path too long)")));
    }

    fill_file_sizes(segment_id, data_dir, fcinfo);

    return (Datum) 0;
}

static int get_file_sizes_for_databases(List *databases_ids) {
    int retcode = 0;
    char query[MAX_QUERY_SIZE];
    ListCell *current_cell;

    /* connect to SPI */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        ereport(ERROR, (errmsg("get_file_sizes_for_databases: SPI_connect failed")));
    }

    foreach (current_cell, databases_ids) {
        int dbid = lfirst_int(current_cell);
        sprintf(query, "INSERT INTO gp_toolkit.segment_file_sizes (segment, relfilenode, filepath, size, mtime) \
                SELECT * from get_file_sizes_for_database(%d)",
                dbid);

        /* execute sql query to create table (if it not exists) */
        retcode = SPI_execute(query, false, 0);

        /* check errors if they're occured during execution */
        if (retcode != SPI_OK_INSERT) { /* error */
            SPI_finish();
            ereport(ERROR, (errmsg("get_file_sizes_for_databases: SPI_execute failed (insert into segment_file_sizes)")));
        }
    }

    SPI_finish();
    return retcode;
}

Datum collect_table_sizes(PG_FUNCTION_ARGS) {
    bool elem_type_by_val;
    bool *args_nulls;
    char elem_alignment_code;
    int16 elem_width;
    int args_count;
    ArrayType *ignored_db_names_array;
    Datum *args_datums;
    List *ignored_db_names = NIL, *databases_ids;
    Oid elem_type;

    // put all ignored_db names from fisrt array-argument
    ignored_db_names_array = PG_GETARG_ARRAYTYPE_P(0);
    elem_type = ARR_ELEMTYPE(ignored_db_names_array);
    get_typlenbyvalalign(elem_type, &elem_width, &elem_type_by_val, &elem_alignment_code);
    deconstruct_array(ignored_db_names_array, elem_type, elem_width, elem_type_by_val, elem_alignment_code,
                      &args_datums, &args_nulls, &args_count);
    for (int i = 0; i < args_count; ++i) {
        ignored_db_names =
            lappend(ignored_db_names, (void *)DatumGetCString(DirectFunctionCall1(textout, args_datums[i])));
    }

    databases_ids = get_collectable_db_ids(ignored_db_names, fcinfo);

    get_file_sizes_for_databases(databases_ids);

    PG_RETURN_VOID();
}

void _PG_init(void) {
    // nothing to do here for this template, but usually we register hooks here,
    // allocate shared memory, start background workers, etc
}

void _PG_fini(void) {
    // nothing to do here for this template
}
