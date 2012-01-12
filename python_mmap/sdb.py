# -*- coding: UTF-8 -*-
# File: sdb.py
# Date: 2011-11-08
# Author: gashero

"""
Stratosphere Database implementation by python mmap module.
"""

import re
import os
import mmap
import struct

## Internal Implementation #####################################################

class NotFoundError(Exception):

    def __init__(self,errmsg,errnum=0):
        self.errmsg=errmsg
        self.errnum=errnum
        self.args=(errnum,errmsg)
        return

    def __repr__(self):
        return 'NotFoundError(%s,%d)'%(repr(self.errmsg),self.errnum)

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
        self.redolog_filename=redolog
        self._loaddb()
        self.redolist=[]
        self.redolog=open(self.redolog_filename,'a+')
        return

    def __del__(self):
        self.indexfile.close()
        self.chunkfile.close()
        return

    def _loaddb(self):
        if not os.path.exists(self.redolog_filename):
            return
        _redolog=open(self.redolog_filename,'rb')
        environ={
                'set_record':   self._set_rec,
                'set_chunk':    self._set_chunk,
                'comment':      lambda x:None,
                }
        for line in _redolog.xreadlines():
            redolist=eval(line)
            for redo in redolist:
                eval(redo,environ)
        _redolog.close()
        return

    def _get_rec(self,pos):
        rec=list(INDEX_RECORD.unpack(self.indexfile[pos*64:(pos+1)*64]))
        return rec

    def _set_rec(self,pos,rec,nolog=False):
        assert len(rec)==8
        self.indexfile[pos*64:(pos+1)*64]=INDEX_RECORD.pack(*rec)
        if not nolog:
            self.redolist.append('set_record(%d,%s,True)'%(pos,repr(rec)))
        return

    def _set_chunk(self,pos,s,nolog=False):
        chunk=struct.pack('Q',len(s))+s+'\n'
        self.chunkfile[pos:pos+len(s)+9]=chunk
        if not nolog:
            self.redolist.append('set_chunk(%d,%s,True)'%(pos,repr(s)))
        return

    def commit(self,log=None):
        if log:
            self.redolog.append('comment(%s)'%repr(log))
        self.redolog.write(repr(self.redolist)+'\n')
        self.redolog.flush()
        self.redolist=[]
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

    def set_pointer(self,rid_from,rid_to):
        objpos=rid_from
        while True:
            rec=self._get_rec(objpos)
            dataset=[]
            try:
                blankpos=rec[1:-1].index(0)
                rec[blankpos+1]=rid_to
                break
            except ValueError:
                if rec[-1]!=0:
                    objpos=rec[-1]
                else:
                    raise NotFoundError('rid_to1 not found')
        self._set_rec(objpos,rec)
        return

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

def redolog_encode(data):
    line=data
    ESCAPE_1CHAR=re.compile('[\t\n\f\r\b]')
    line=ESCAPE_1CHAR.sub(
            lambda m:{
                '\t':'\\t',
                '\n':'\\n',
                '\f':'\\f',
                '\r':'\\r',
                '\b':'\\b',
                }.__getitem__(m.group()),
            data)
    return line

def redolog_decode(line):
    ESCAPE_1CHAR=re.compile("\\\\[tnfrb]")
    data=ESCAPE_1CHAR.sub(
            lambda m:{
                '\\t':'\t',
                '\\n':'\n',
                '\\f':'\f',
                '\\r':'\r',
                '\\b':'\b',
                }.__getitem__(m.group()),
            line)
    return data

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
        os.unlink('/tmp/redo.log')
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
        self.assertEqual(self.sdbr.locate_record('name','hliu'),2)
        self.assertEqual(self.sdbr.prefix_pointer(rid_harry,'age='),['age=29','age=30'])
        self.assertEqual(self.sdbr.prefix_pointer(rid_harry),['age=29','age=30','sex=male'])
        self.sdbw.delete_pointer(rid_harry,rid_age29)
        self.sdbw.set_pointer(rid_harry,rid_age31)
        self.assertEqual(self.sdbr.prefix_pointer(rid_harry,'age='),['age=30','age=31'])
        self.sdbw.delete_pointer(rid_harry,rid_age31)
        self.assertEqual(self.sdbr.prefix_pointer(rid_harry,'age='),['age=30'])
        return

    def test_commit_loaddb(self):
        rid_harry=self.sdbw.new_record('name','harry')
        self.sdbw.commit()
        rid_hliu=self.sdbw.new_record('name','hliu')
        self.sdbw.commit()
        rid_age29=self.sdbw.new_record('age','29')
        self.sdbw.commit()
        rid_age30=self.sdbw.new_record('age','30')
        self.sdbw.commit()
        rid_male=self.sdbw.new_record('sex','male')
        self.sdbw.commit()
        rid_age31=self.sdbw.new_record('age','31')
        self.sdbw.commit()
        self.sdbw.append_pointer(rid_harry,rid_age29)
        self.sdbw.commit()
        self.sdbw.append_pointer(rid_harry,rid_age30)
        self.sdbw.commit()
        self.sdbw.append_pointer(rid_harry,rid_male)
        self.sdbw.commit()
        self.sdbw.append_pointer(rid_hliu,rid_age29)
        self.sdbw.commit()
        self.sdbw.delete_pointer(rid_harry,rid_age29)
        self.sdbw.commit()
        self.sdbw.set_pointer(rid_harry,rid_age31)
        self.sdbw.commit()
        self.sdbw.delete_pointer(rid_harry,rid_age31)
        self.sdbw.commit()
        os.system('cp /tmp/redo.log /tmp/bkredo.log')
        del self.sdbw
        del self.sdbr
        self.sdbw=SDBWriter('/tmp/redo.log',1024*2,1024*2)
        self.sdbr=SDBReader(1024*2,1024*2)
        self.assertEqual(self.sdbr.locate_record('name','harry'),1)
        self.assertEqual(self.sdbr.locate_record('name','hliu'),2)
        self.assertEqual(self.sdbr.prefix_pointer(rid_harry,'age='),['age=30'])
        return

class TestRedoLog(unittest.TestCase):

    def test_redolog_encode_decode(self):
        data='hello\tworld\nhehe\r\ftest\b\n'
        line=redolog_encode(data)
        self.assertEqual(line,'hello\\tworld\\nhehe\\r\\ftest\\b\\n')
        self.assertEqual(redolog_decode(line),data)
        return

if __name__=='__main__':
    unittest.main()
