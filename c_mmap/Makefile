# File: Makefile
# Date: 2012-03-09
# Author: gashero <harry.python@gmail.com>

CC=gcc
CFLAGS=-Wall
COMPILE=$(CC) $(CFLAGS) -c
TARGET=stratosphere_db
OBJS=sdb_core.o sdb_cli.o

.PHONY: all clean

$(TARGET): $(OBJS)
	$(CC) -o $@ $^

.c.o:
	$(COMPILE) -o $@ $<

.SUFFIXES: .c .o

clean:
	rm $(TARGET)
	rm -f *.o
