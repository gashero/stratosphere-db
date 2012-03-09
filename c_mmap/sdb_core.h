/*
 * File: sdb_core.h
 * Date: 2012-03-09
 * Author: gashero <harry.python@gmail.com>
 */

#ifndef SDB_CORE_H
#define SDB_CORE_H

#define POINTERS_PER_NODE   8

typedef struct pointers {
    void *pointers[POINTERS_PER_NODE];
} pointers;

typedef struct index_node{
    void        *p_data;
    struct index_node   *p_left;
    struct index_node   *p_right;
    pointers    *p_pointers;
} index_node;

#endif
