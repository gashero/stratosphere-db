=====================================================
Stratosphere Database Test Implementation by Python
=====================================================

:Author: gashero <harry.python@gmail.com>
:Date: 2011-11-09

Introduction
--------------

Python is a good choice implement a prototype of any software. Stratosphere database is a complex software, so I will try it in python.

Data Model
------------

The data model of Stratosphere Database is directed graph. Every node just a key=value pair, for example "name=harry".

Interfaces
------------

``node=get_node(nodename)``

Query and fetch **a** node by node name equal "key=value". If there is no node named nodename, this function will return None.

``fieldlist=get_fieldlist(node, fieldname=None)``

Get a node's fieldlist, filter by fieldname. if fieldname is None, DO NOT filter the fieldlist.

Fieldlist is a list of field, field is format "key=value".

If there is no filed match the fieldname or the node has not a field. This function will return a empty list "[]".

``fieldlist=filter_fieldlist(node, filter)``

@wait

``node=new_node(nodename)``

Create a new node named nodename, and return the node object. If there is a node named nodename, it just return the node, not create the node.

``insert_field(node, filed)``

@wait

``append_filed(node, filed)``

Append the field to the node's fielelist end.

``delete_field(node, filed)``

Delete the filed in node, totally match the filed. Not assign the position. If there is more than one field match the field, it will delete the first match.
