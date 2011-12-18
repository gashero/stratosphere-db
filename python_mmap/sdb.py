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

class SDBWriter(object):

    def __init__(self, redolog, dbsize, dbfile='/tmp/hello.sdb'):
        self.mf=os.open(dbfile, os.O_CREAT|os.O_TRUNC|os.O_RDWR)
        os.write(self.mf,'\x00'*dbsize) #may be too large
        self.mm=mmap.mmap(self.mf, dbsize, mmap.MAP_SHARED, mmap.PROT_WRITE)
        return

    def __del__(self):
        return

    def logit(self,logline):
        return

class SDBReader(object):

    def __init__(self, dbsize, dbfile='/tmp/hello.sdb'):
        self.mf=os.open(dbfile,os.O_RDONLY)
        self.mm=mmap.mmap(self.mf, dbsize, mmap.MAP_SHARED, mmap.PROT_READ)
        return

## Unittest ####################################################################

import unittest
import time

class TestOthers(unittest.TestCase):

    def setUp(self):
        return

    def tearDown(self):
        return

    def test_big_db(self):
        """Test a large mmap file, confirm it works

        In my macmini mb463, 1GB mmap file need 14-27 second
        """
        time_start=time.time()
        sdbw=SDBWriter('/tmp/redo.log',1024**3)
        time_finish=time.time()
        #print time_finish-time_start
        return

if __name__=='__main__':
    unittest.main()
