/*
 * File: sdb_core.c
 * Date: 2012-03-09
 * Author: gashero <harry.python@gmail.com>
 */

#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>

#include "sdb_core.h"

sdb_error_t *sdb_open_writer(sdb_t *sdb) {
    return NULL;
}

sdb_error_t *sdb_open_reader(sdb_t *sdb) {
    return NULL;
}

void sdb_close(sdb_t *sdb) {
}

/*
 * Assign a struct with error number & message
 */
sdb_error_t *sdb_error_new(int errnum, char *errmsg) {
    sdb_error_t *error=(sdb_error_t*)malloc(sizeof(sdb_error_t));
    error->errnum=errnum;
    error->errmsg=(char*)malloc(sizeof(strlen(errmsg)));
    strcpy(error->errmsg,errmsg);
    return error;
}

/*
 * Free resource assigned by sdb_error_new()
 */
void sdb_error_del(sdb_error_t *error) {
    free(error->errmsg);
    free(error);
}
