#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(int_square_c_impl);

void _PG_init(void);
void _PG_fini(void);

Datum int_square_c_impl(PG_FUNCTION_ARGS) {
  int64 in = DatumGetInt64(PG_GETARG_DATUM(0));
  PG_RETURN_INT64(in * in);
}

void _PG_init(void) {
  // nothing to do here for this template, but usually we register hooks here,
  // allocate shared memory, start background workers, etc
}

void _PG_fini(void) {
  // nothing to do here for this template
}