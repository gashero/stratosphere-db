/*
 * File: keypointer.h
 * Date: 2011-09-01
 * Author: gashero
 */

#ifndef KEYPOINTER_H
#define KEYPOINTER_H

typedef struct {
} keypointer_database;

typedef struct {
    keypointer_node     *node;
    keypointer_relate   *next;
} keypointer_relate;

typedef struct {
    char                *name;
    keypointer_relate   *plist;
} keypointer_node;

keypointer_node *query_keypointer_node(char *keyname);
keypointer_node *make_keypointer_node(char *keyname);
keypointer_node *set_link_keypointer_node(keypointer_node *node_from, keypointer_node *node_to);
keypointer_node *remove_link_keypointer_node(keypointer_node *node_from, keypointer_node *node_to);

#endif
