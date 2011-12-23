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
        assert len(rec)==8
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
        #self.indexfile.flush()
        #self.chunkfile.flush()
        return index_id

    def append_pointer(self,rid_from,rid_to):
        objpos=rid_from
        #get last record
        while True:
            rec=self._get_rec(objpos)
            if rec[-1]!=0:
                objpos=rec[-1]
            else:
                break
        rec=filter(lambda x:x!=0,rec)
        if len(rec)<7:
            rec.append(rid_to)
            rec+=[0]*(8-len(rec))
            self._set_rec(objpos,rec)
        else:           #record is full
            #inc index id
            meta_record=self._get_rec(0)
            index_id=meta_record[0]
            meta_record[0]=index_id+1
            self._set_rec(0,meta_record)
            rec[-1]=index_id
            self._set_rec(objpos,rec)
            new_ext_rec=[-1,rid_to,0,0,0,0,0]
            self._set_rec(index_id,new_ext_rec)
        return

    def update_pointer(self,rid_from,rid_to1,rid_to2):
        objpos=rid_from
        while True:
            rec=self._get_rec(objpos)
            dataset=[]
            if rid_to1 in rec[1:-1]:
                for iid in rec[1:-1]:
                    if iid==rid_to1:
                        dataset.append(rid_to2)
                    else:
                        dataset.append(iid)
                rec=rec[:1]+dataset+rec[-1:]
                break
            else:
                if rec[-1]!=0:
                    objpos=rec[-1]
                else:
                    raise NotFoundError('rid_to1 not found')
        self._set_rec(objpos,rec)
        return

    def delete_pointer(self,rid_from,rid_to):
        objpos=rid_from
        while True:
            rec=self._get_rec(objpos)
            if rid_to in rec[1:-1]:
                rec=rec[:1]+filter(lambda x:x!=rid_to,rec[1:-1])+[0]+rec[-1:]
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

    def _filter_pointer(self,rid,func):
        objpos=rid
        retlist=[]
        while True:
            rec=self._get_rec(objpos)
            for iid in rec[1:-1]:
                if iid==0:
                    continue
                prec=self._get_rec(iid)
                s=self._get_chunk(prec[0])
                #print prec,repr(s)
                if func(s):
                    retlist.append(s)
            if rec[-1]!=0:
                objpos=rec[-1]
            else:
                break
        return retlist

    def locate_record(self,key,value):
        pos=1
        s=key+'='+value
        while True:
            rec=self._get_rec(pos)
            if rec==[0,0,0,0,0,0,0,0]:
                break
            if rec[0]==-1:
                pos+=1
                continue
            chunkpos=rec[0]
            chunk=self._get_chunk(chunkpos)
            #print 'chunk=',repr(chunk)
            if chunk==s:
                return pos
            pos+=1
        raise NotFoundError('key&value not exists')

    def prefix_pointer(self,rid,prefix=None):
        if prefix:
            retlist=self._filter_pointer(rid,lambda x:x.startswith(prefix))
        else:
            retlist=self._filter_pointer(rid,lambda x:True)
        return retlist

    def regex_pointer(self,rid,regex=None):
        pattern=re.compile(regex)
        retlist=self._filter_pointer(rid,lambda x:pattern.match(x))
        return retlist

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

class TestSDBWriterReader(unittest.TestCase):

    def setUp(self):
        self.sdbw=SDBWriter('/tmp/redo.log',1024*2,1024*2)
        self.sdbr=SDBReader(1024*2,1024*2)
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
        self.assertEqual(self.sdbr.locate_record('name','h4'),5)
        return

    def test_append_update_delete_pointer(self):
        rid_harry=self.sdbw.new_record('name','harry')
        rid_hliu=self.sdbw.new_record('name','hliu')
        rid_age29=self.sdbw.new_record('age','29')
        rid_age30=self.sdbw.new_record('age','30')
        rid_male=self.sdbw.new_record('sex','male')
        rid_age31=self.sdbw.new_record('age','31')
        self.sdbw.append_pointer(rid_harry,rid_age29)
        self.sdbw.append_pointer(rid_harry,rid_age30)
        self.sdbw.append_pointer(rid_harry,rid_male)
        self.sdbw.append_pointer(rid_hliu,rid_age29)
        self.assertEqual(self.sdbw._get_rec(rid_harry)[1:],
                [rid_age29,rid_age30,rid_male,0,0,0,0])
        self.assertEqual(self.sdbw._get_rec(rid_hliu)[1:],
                [rid_age29,0,0,0,0,0,0])
        self.assertEqual(self.sdbr.locate_record('name','harry'),1)
        self.assertEqual(self.sdbr.prefix_pointer(rid_harry,'age='),['age=29','age=30'])
        self.assertEqual(self.sdbr.prefix_pointer(rid_harry),['age=29','age=30','sex=male'])
        self.sdbw.update_pointer(rid_harry,rid_age29,rid_age31)
        self.assertEqual(self.sdbr.prefix_pointer(rid_harry,'age='),['age=31','age=30'])
        self.sdbw.delete_pointer(rid_harry,rid_age31)
        self.assertEqual(self.sdbr.prefix_pointer(rid_harry,'age='),['age=30'])
        return

if __name__=='__main__':
    unittest.main()
