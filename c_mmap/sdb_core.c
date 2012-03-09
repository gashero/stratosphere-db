/*
 * File: sdb_core.c
 * Date: 2012-03-09
 * Author: gashero <harry.python@gmail.com>
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include "sdb_core.h"

sdb_error_t *sdb_open_writer(sdb_t *sdb) {
    if ((sdb->f_index.fd=open(
                    sdb->f_index.filename,O_CREAT|O_TRUNC|O_RDWR))<0) {
        return sdb_error_new(errno,"open('index') failed",
                (long)sdb->f_index.fd);
    }
    //TODO:continue work
    return NULL;
}

sdb_error_t *sdb_open_reader(sdb_t *sdb) {
    if ((sdb->f_index.fd=open(
                    sdb->f_index.filename,O_RDONLY))<0) {
        return sdb_error_new(errno,"open('index') failed",
                (long)sdb->f_index.fd);
    }
    if ((sdb->f_pointer.fd=open(
                    sdb->f_pointer.filename,O_RDONLY))<0) {
        return sdb_error_new(errno,"open('pointer') failed",
                (long)sdb->f_pointer.fd);
    }
    if ((sdb->f_chunk.fd=open(
                    sdb->f_chunk.filename,O_RDONLY))<0) {
        return sdb_error_new(errno,"open('chunk') failed",
                (long)sdb->f_chunk.fd);
    }
    if ((sdb->f_index.addr=mmap(NULL,sdb->f_index.maxsize,PROT_READ,
                    MAP_SHARED,sdb->f_index.fd,0))==(void*)-1) {
        /* TODO:I'm not sure convert -1 to (void*) work fine.*/
        return sdb_error_new(errno,"mmap('index') failed",
                (long)sdb->f_index.addr);
    }
    if ((sdb->f_pointer.addr=mmap(NULL,sdb->f_pointer.maxsize,PROT_READ,
                    MAP_SHARED,sdb->f_pointer.fd,0))==(void*)-1) {
        return sdb_error_new(errno,"mmap('pointer') failed",
                (long)sdb->f_pointer.addr);
    }
    if ((sdb->f_chunk.addr=mmap(NULL,sdb->f_chunk.maxsize,PROT_READ,
                    MAP_SHARED,sdb->f_chunk.fd,0))==(void*)-1) {
        return sdb_error_new(errno,"mmap('chunk') failed",
                (long)sdb->f_chunk.addr);
    }
    close(sdb->f_index.fd);
    close(sdb->f_pointer.fd);
    close(sdb->f_chunk.fd);
    return NULL;
}

sdb_error_t *sdb_close(sdb_t *sdb) {
    int retcode;
    if ((retcode=munmap(sdb->f_index.addr,sdb->f_index.maxsize))<0) {
        return sdb_error_new(errno,"munmap('index') failed",retcode);
    }
    if ((retcode=munmap(sdb->f_pointer.addr,sdb->f_pointer.maxsize))<0) {
        return sdb_error_new(errno,"munmap('pointer') failed",retcode);
    }
    if ((retcode=munmap(sdb->f_chunk.addr,sdb->f_chunk.maxsize))<0) {
        return sdb_error_new(errno,"munmap('chunk') failed",retcode);
    }
    return NULL;
}

/*
 * Assign a struct with error number & message
 */
sdb_error_t *sdb_error_new(int errnum, char *errmsg, int retcode) {
    sdb_error_t *error=(sdb_error_t*)malloc(sizeof(sdb_error_t));
    error->errnum=errnum;
    error->errmsg=(char*)malloc(sizeof(strlen(errmsg)));
    strcpy(error->errmsg,errmsg);
    error->retcode=retcode;
    return error;
}

/*
 * Free resource assigned by sdb_error_new()
 */
void sdb_error_del(sdb_error_t *error) {
    free(error->errmsg);
    free(error);
}

void sdb_error_print(sdb_error_t *error) {
    //TODO:Not Tested!
    fprintf(stderr,"ERROR: %s errno=%d retcode=0x%016lx\n",
            error->errmsg,error->errnum,(unsigned long)error->retcode);
}

void sdb_error_format(sdb_error_t *error) {
    //TODO:continue
}
