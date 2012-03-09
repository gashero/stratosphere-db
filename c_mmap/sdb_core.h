/*
 * File: sdb_core.h
 * Date: 2012-03-09
 * Author: gashero <harry.python@gmail.com>
 */

#ifndef SDB_CORE_H
#define SDB_CORE_H

#define POINTERS_PER_NODE   7

typedef struct sdb_pointer_list_t {
    void *pointer[POINTERS_PER_NODE];
    void *next_pointer_list;
} sdb_pointer_list_t;

typedef struct sdb_index_node_t {
    void        *p_data;
    struct sdb_index_node_t *p_left;
    struct sdb_index_node_t *p_right;
    sdb_pointer_list_t      *p_pointer_list;
} sdb_index_node_t;

typedef struct sdb_mmap_file_info_t {
    char    *filename;
    int     maxsize;
    int     fd;
    void    *addr;
} sdb_mmap_file_info_t;

typedef struct sdb_t {
    sdb_mmap_file_info_t f_index;
    sdb_mmap_file_info_t f_pointer;
    sdb_mmap_file_info_t f_chunk;
    char    *fn_redolog;
    int     fd_redolog;
} sdb_t;

typedef struct sdb_error_t {
    int     errnum;
    char    *errmsg;
    long    retcode;
} sdb_error_t;

sdb_error_t *sdb_open_writer(sdb_t *sdb);
sdb_error_t *sdb_open_reader(sdb_t *sdb);
sdb_error_t *sdb_close(sdb_t *sdb);

sdb_error_t *sdb_error_new(int errnum, char *errmsg, int retcode);
void sdb_error_del(sdb_error_t *error);
void sdb_error_print(sdb_error_t *error);
void sdb_error_format(sdb_error_t *error);

#endif
