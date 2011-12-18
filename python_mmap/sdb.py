# -*- coding: UTF-8 -*-
# File: sdb.py
# Date: 2011-11-08
# Author: gashero

"""
Stratosphere Database implementation by python mmap module.
"""

import os
import mmap
import struct

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

INDEX_RECORD=struct.Struct('QQQQQQQQ')

class SDBWriter(object):

    def __init__(self, redolog, indexsize, chunksize, indexfile='/tmp/hello.sdbi', chunkfile='/tmp/hello.sdbc'):
        self.fd_indexfile=os.open(indexfile, os.O_CREAT|os.O_TRUNC|os.O_RDWR)
        self.fd_chunkfile=os.open(chunkfile, os.O_CREAT|os.O_TRUNC|os.O_RDWR)
        os.write(self.fd_indexfile,'\x00'*indexsize)
        os.write(self.fd_chunkfile,'\x00'*chunksize)
        self.indexfile=mmap.mmap(self.fd_indexfile, indexsize, mmap.MAP_SHARED, mmap.PROT_WRITE)
        self.chunkfile=mmap.mmap(self.fd_chunkfile, chunksize, mmap.MAP_SHARED, mmap.PROT_WRITE)
        self.indexfile[:8]=struct.pack('Q',1)
        return

    def __del__(self):
        self.indexfile.close()
        self.chunkfile.close()
        return

    def logit(self,logline):
        return

    def get_next_id(self):
        meta_record=INDEX_RECORD.unpack(self.indexfile[:64])
        next_index_id=meta_record[0]+1
        meta_record=list(meta_record)
        meta_record[0]=next_index_id
        self.indexfile[:64]=INDEX_RECORD.pack(*meta_record)
        return next_index_id-1

    def new_record(self,key,value,rid):
        s=key+'='+value

    def update_pointer(self,rid_from,rid_to):
        #TODO:
        return

    def append_pointer(self,rid_from,rid_to):
        #TODO:
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

    def _test_big_db(self):
        """Test a large mmap file, confirm it works

        In my macmini mb463, 1GB mmap file need 14-27 second
        """
        time_start=time.time()
        sdbw=SDBWriter('/tmp/redo.log',1024**3)
        time_finish=time.time()
        #print time_finish-time_start
        return

class TestSDBWriter(unittest.TestCase):

    def setUp(self):
        self.sdbw=SDBWriter('/tmp/redo.log',1024**2,1024**2)
        return

    def tearDown(self):
        del self.sdbw
        return

    def test_get_next_id(self):
        self.assertEqual(self.sdbw.get_next_id(),1)
        self.assertEqual(self.sdbw.get_next_id(),2)
        self.assertEqual(self.sdbw.indexfile[:8],'\x03\x00\x00\x00\x00\x00\x00\x00')
        return

if __name__=='__main__':
    unittest.main()
