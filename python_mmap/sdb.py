# -*- coding: UTF-8 -*-
# File: sdb.py
# Date: 2011-11-08
# Author: gashero

"""
Stratosphere Database implementation by python mmap module.
"""

import os
import mmap

## Interface ###################################################################

def get_node(nodename):
    return

def get_fieldlist(node, filedname=None):
    return

def new_node(nodename):
    return

def append_field(node, field):
    return

def delete_field(node, field):
    return

## Internal Implementation #####################################################

class SDB(object):

    def __init__(self, redolog, dbsize, dbfile='/tmp/hello.sdb'):
        self.mf=os.open(dbfile,
                os.O_CREAT|os.O_TRUNC|os.O_RDWR)
        self.mf.write('\x00'*dbsize)    #may be too large
        self.mm=mmap.mmap(self.mf, dbsize, mmap.MAP_SHARED, mmap.PROT_WRITE)
        return

    def __del__(self):
        return

    def logit(self,logline):
        return

class SDBClient(object):
    pass
