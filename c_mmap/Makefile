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

%.o: %.c %.h
	$(COMPILE) -o $@ $<

clean:
	rm -f $(TARGET)
	rm -f *.o
