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

class NotFoundError(Exception):

    def __init__(self,errmsg,errnum=0):
        self.errmsg=errmsg
        self.errnum=errnum
        self.args=(errnum,errmsg)
        return

    def __repr__(self):
        return 'NotFoundError(%s,%d)'%(repr(self.errmsg),self.errnum)

## Internal Implementation #####################################################

INDEX_RECORD=struct.Struct('QQQQQQQQ')

class SDBWriter(object):

    def __init__(self, redolog, indexsize, chunksize,
            indexfile='/tmp/hello.sdbi', chunkfile='/tmp/hello.sdbc'):
        self.fd_indexfile=os.open(indexfile, os.O_CREAT|os.O_TRUNC|os.O_RDWR)
        self.fd_chunkfile=os.open(chunkfile, os.O_CREAT|os.O_TRUNC|os.O_RDWR)
        os.write(self.fd_indexfile,'\x00'*indexsize)
        os.write(self.fd_chunkfile,'\x00'*chunksize)
        self.indexfile=mmap.mmap(self.fd_indexfile, indexsize,
                mmap.MAP_SHARED, mmap.PROT_WRITE)
        self.chunkfile=mmap.mmap(self.fd_chunkfile, chunksize,
                mmap.MAP_SHARED, mmap.PROT_WRITE)
        self.indexfile[:16]=struct.pack('QQ',1,1)
        return

    def __del__(self):
        self.indexfile.close()
        self.chunkfile.close()
        return

    def logit(self,logline):
        return

    def _get_rec(self,pos):
        rec=list(INDEX_RECORD.unpack(self.indexfile[pos*64:(pos+1)*64]))
        return rec

    def _set_rec(self,pos,rec):
        self.indexfile[pos*64:(pos+1)*64]=INDEX_RECORD.pack(*rec)
        return

    def _set_chunk(self,pos,s):
        chunk=struct.pack('Q',len(s))+s+'\n'
        self.chunkfile[pos:pos+len(s)+9]=chunk
        return

    def new_record(self,key,value):
        s=key+'='+value
        #inc index id & chunk id
        meta_record=self._get_rec(0)
        index_id=meta_record[0]
        chunk_id=meta_record[1]
        meta_record[0]=index_id+1
        meta_record[1]=chunk_id+len(s)+8+1
        self._set_rec(0,meta_record)
        #write chunk
        self._set_chunk(chunk_id,s)
        #write index
        vertex_record=[chunk_id,0,0,0,0,0,0,0]
        self._set_rec(index_id,vertex_record)
        self.indexfile.flush()
        self.chunkfile.flush()
        return index_id

    def update_pointer(self,rid_from,rid_to1,rid_to2):
        objpos=rid_from
        while True:
            rec=self._get_rec(objpos)
            for (pos,iid) in zip(range(1,6),rec[1:-1]):
                if iid==rid_to1:
                    rec[pos]==rid_to2
                    break
            else:
                if rec[-1]!=0:
                    objpos=rec[-1]
                else:
                    raise NotFoundError('rid_to1 not found')
        self._set_rec(objpos,rec)
        return

    def append_pointer(self,rid_from,rid_to):
        objpos=rid_from
        while True:
            rec=self._get_rec(objpos)
            if rec[-1]!=0:
                objpos=rec[-1]
            else:
                break
        for pos in range(6,1):
            if rec[pos]!=0:
                break
        if pos!=6:  #record is not full
            rec[pos]=rid_to
            self._set_rec(objpos,rec)
        else:
            #inc index id
            meta_record=self._get_rec(0)
            index_id=meta_record[0]
            meta_record[0]=index_id+1
            self._set_rec(0,meta_record)
            rec[-1]=index_id
            self._set_rec(objpos,rec)
            new_ext_rec=[0,rid_to,0,0,0,0,0]
            self._set_rec(index_id,new_ext_rec)
        return

    def delete_pointer(self,rid_from,rid_to):
        objpos=rid_from
        while True:
            rec=self._get_rec(objpos)
            for (pos,iid) in zip(range(1,6),rec[1:6]):
                if iid==rid_to:
                    rec[pos]=0
                    break
            else:
                if rec[-1]!=0:
                    objpos=rec[-1]
                else:
                    raise NotFoundError('rid_to not found')
        self._set_rec(objpos,rec)
        return

class SDBReader(object):

    def __init__(self, indexsize, chunksize,
            indexfile='/tmp/hello.sdbi', chunkfile='/tmp/hello.sdbc'):
        self.fd_indexfile=os.open(indexfile,os.O_RDONLY)
        self.fd_chunkfile=os.open(chunkfile,os.O_RDONLY)
        self.indexfile=mmap.mmap(self.fd_indexfile, indexsize,
                mmap.MAP_SHARED, mmap.PROT_READ)
        self.chunkfile=mmap.mmap(self.fd_chunkfile, chunksize,
                mmap.MAP_SHARED, mmap.PROT_READ)
        return

    def __del__(self):
        self.indexfile.close()
        self.chunkfile.close()
        return

    def _get_rec(self,pos):
        rec=list(INDEX_RECORD.unpack(self.indexfile[pos*64:(pos+1)*64]))
        return rec

    def _get_chunk(self,pos):
        chunklen=struct.unpack('Q',self.chunkfile[pos:pos+8])[0]
        chunk=self.chunkfile[pos+8:pos+chunklen+8]
        return chunk

    def locate_record(self,key,value):
        pos=1
        s=key+'='+value
        while True:
            rec=self._get_rec(pos)
            if rec==[0,0,0,0,0,0,0,0]:
                break
            chunkpos=rec[0]
            chunk=self._get_chunk(chunkpos)
            #print 'chunk=',repr(chunk)
            if chunk==s:
                return pos
            pos+=1
        raise NotFoundError('key&value not exists')

    def prefix_pointer(self,key,value,prefix=None):
        #TODO:
        return

    def regex_pointer(self,key,value,regex=None):
        #TODO:
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
        self.sdbr=SDBReader(1024**2,1024**2)
        return

    def tearDown(self):
        del self.sdbw
        del self.sdbr
        return

    def test_get_set_rec(self):
        self.sdbw._set_rec(1,range(2,10))
        self.sdbw._set_rec(2,range(3,11))
        self.sdbw._set_rec(3,range(4,12))
        self.sdbw._set_rec(4,range(5,13))
        self.assertEqual(self.sdbw._get_rec(1),range(2,10))
        self.assertEqual(self.sdbw._get_rec(4),range(5,13))
        self.assertEqual(self.sdbw._get_rec(2),range(3,11))
        self.assertEqual(self.sdbw._get_rec(3),range(4,12))
        return

    def test_new_record(self):
        self.sdbw.new_record('name','harry')
        self.sdbw.new_record('name','hliu')
        self.sdbw.new_record('name','h2')
        self.sdbw.new_record('name','h3')
        self.sdbw.new_record('name','h4')
        self.sdbw.new_record('name','h5')
        #for i in range(8):
        #    print '%d\t%s\t%s'%(i,repr(self.sdbw._get_rec(i)),
        #        self.sdbr._get_rec(i))
        self.assertEqual(self.sdbr.locate_record('name','hliu'),2)
        return

    def test_update_pointer(self):
        #TODO:
        return

    def test_append_pointer(self):
        #TODO:
        return

    def test_delete_pointer(self):
        #TODO:
        return

if __name__=='__main__':
    unittest.main()
