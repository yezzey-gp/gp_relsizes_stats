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

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(int_square_c_impl);
PG_FUNCTION_INFO_V1(collect_table_size);

typedef struct segment_request_data {
    Datum *contents;
    Datum *hostnames;
    Datum *ports;
    Datum *datadirs;
} segment_request_data_t;

void _PG_init(void);
void _PG_fini(void);
static List* get_collectable_db_ids(List *ignored_db_names);
static int create_trunkate_table();
static segment_request_data_t* get_segments_basedirs();
static void load_segment_files(char *path, char* csv_path, int segment);
static void load_all_segments_stats(char *dest_dir, int dboid);

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

static segment_request_data_t* get_segments_basedirs() {
    int retcode = 0;
    segment_request_data_t *packed_data = NULL;
    char *sql = "SELECT content, hostname, port, datadir \
                FROM gp_segment_configuration \
                WHERE role = 'p' \
                ORDER BY port"; 
    /* connect to SPI */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        elog(ERROR, "get_segments_basedirs: SPI_connect returned %d", retcode);
        goto finish_SPI;
    } 

    /* execute sql query to get table */
    retcode = SPI_execute(sql, true, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_SELECT || SPI_processed < 0) {
        elog(ERROR, "get_segments_basedirs: SPI_execute returned %d, processed %lu rows", retcode, SPI_processed);
        goto finish_SPI;
    }
    
    packed_data = palloc0(sizeof(*packed_data));
    packed_data->contents = palloc0(SPI_processed * sizeof(*packed_data->contents));
    packed_data->hostnames = palloc0(SPI_processed * sizeof(*packed_data->hostnames));
    packed_data->ports = palloc0(SPI_processed * sizeof(*packed_data->ports));
    packed_data->datadirs = palloc0(SPI_processed * sizeof(*packed_data->datadirs));

    Datum *tuple_values = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_values));
    bool *tuple_nullable = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_nullable));
    
    for (int i = 0; i < SPI_processed; ++i) {
        HeapTuple current_tuple = SPI_tuptable->vals[i];
        heap_deform_tuple(current_tuple, SPI_tuptable->tupdesc, tuple_values, tuple_nullable);

        packed_data->contents[i] = tuple_values[0];
        packed_data->hostnames[i] = tuple_values[1];
        packed_data->ports[i] = tuple_values[2];
        packed_data->datadirs[i] = tuple_values[3];
    }

    pfree(tuple_values);
    pfree(tuple_nullable);

finish_SPI:
    /* finish SPI */
    SPI_finish();
    return packed_data;
}

static void clear_dest_dir(char *dest_path) {
    struct stat stb;
    struct dirent *file;
    char new_path[PATH_MAX];
    DIR *csv_dir = AllocateDir(dest_path);
    if (!csv_dir) {
        return;
    }
    while ((file = ReadDir(csv_dir, dest_path)) != NULL) {
        char *filename = file->d_name; 
        /* if limit of PATH_MAX reached skip file */
        if (sprintf(new_path, "%s/%s", dest_path, filename) >= sizeof(new_path)) {
            continue;
        }

        /* do lstat if returned error => continue */
        if (lstat(new_path, &stb) < 0 && S_ISREG(stb.st_mode) && 
                (strncmp(filename, "files_", 6) == 0 || strncmp(filename, "map_", 4) == 0)) { // TODO more clear way to check prefix?
            /* remove file with old csv data */
            remove(new_path);
        }
    }
    FreeDir(csv_dir);
}

static void load_segment_files(char *path, char* csv_path) {
    /* if {path} is NULL => return */
    if (!path) {
        return;
    }

    char new_path[PATH_MAX];
    DIR *current_dir = AllocateDir(path);
    /* if {current_dir} did not opened => return */
    if (!current_dir) {
        return;
    }

    struct dirent *file;
    /* start itterating in {current_dir} */
    while ((file = ReadDir(current_dir, path)) != NULL) {
        char *filename = file->d_name;
        /* if filename is special as "." or ".." => continue */
        if (strcmp(filename, ".") == 0 || strcmp(filename, "..") == 0) {
            continue;
        }

        /* if limit of PATH_MAX reached skip file */
        if (sprintf(new_path, "%s/%s", path, filename) >= sizeof(new_path)) {
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
            // can store a lot of different stats here (lstat sys call is MVP)
            FILE *csv_ptr = AllocateFile(csv_path, "a");
            fprintf(csv_ptr, "%d,%s,%ld,%ld\n", GpIdentity.segindex, filename,
                    stb.st_size, stb.st_mtime);
            FreeFile(csv_ptr);
        } else if (S_ISDIR(stb.st_mode)) {
            load_segment_files(new_path, csv_path, segment);
        }
    }
    FreeDir(current_dir);
}

static void load_segment_map(Datum host, Datum port) {
    return;
}

static void load_segment_stats(int dboid, Datum segment, Datum host, Datum port, Datum base_dir, char *dest_dir) {
    char filepath_files[1000]; // TODO put 1000 in constant
    char filepath_map[1000]; // TODO put 1000 in constant
    char final_base_dir[1000]; // TODO put correct value in constant and change 1000 to it
    
    /* create csv files and put headers in them */
    sprintf(filepath_files, "%s/files_gpseg%d.csv", dest_dir, DatumGetInt32(segment));
    FILE *csv_ptr = AllocateFile(filepath_files, "w");
    fprintf(csv_ptr, "startTime, segment, filename, size, lastModified\n");
    FreeFile(csv_ptr);

    sprintf(filepath_map, "%s/map_gpseg%d.csv", dest_dir, DatumGetInt32(segment));
    csv_ptr = AllocateFile(filepath_map, "w");
    fprintf(csv_ptr, "\n"); // TODO
    FreeFile(csv_ptr);

    // TODO prepare correct base dir "{base_dir}/base/{dboid}"
    sprintf(final_base_dir, "%s/base/%d", DatumGetCString(base_dir), dboid);
    
    TimestampTz start_time = GetCurrentTimestamp();
    load_segment_files(final_base_dir, filepath_files, DatumGetInt32(segment));
    load_segment_map(segment, host, port);
    TimestampTz finish_time = GetCurrentTimestamp();
}


static void load_all_segments_stats(char *dest_dir, int dboid) {
    // get segment basedirs
    segment_request_data_t *segment_data = get_segments_basedirs();

    // clear segment base dir from old csv
    clear_dest_dir(dest_dir);

    int segments_cnt = sizeof(segment_data->contents);
    // itterate in segment base_dirs + segment names
    for (int i = 0; i < segments_cnt; ++i) {
        // starts load_segment_stats(_ssh??) function
        load_segment_stats(dboid, segment_data->contents[i], segment_data->hostnames[i], segment_data->ports[i],
                segment_data->datadirs[i], dest_dir);
    }
}

static int write_collected_data(char *path, int dboid) {
    if (!path) {
        return 0;
    }
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
        // prepare csv
        load_all_segments_stats(base_dir, dboid);

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
