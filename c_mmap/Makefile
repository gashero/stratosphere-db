# File: Makefile
# Date: 2012-03-09
# Author: gashero <harry.python@gmail.com>

CC=gcc
CFLAGS=-Wall
COMPILE=$(CC) $(CFLAGS) -c
TARGET=stratosphere_db
OBJS=sdb_core.o sdb_cli.o
LIBS=

.PHONY: all clean

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

%.o: %.c %.h
	$(COMPILE) -o $@ $<

clean:
	rm -f $(TARGET)
	rm -f *.o
