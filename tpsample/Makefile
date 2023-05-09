CC=gcc
CFLAGS=-g -std=gnu11 -Wall
LDFLAGS=-pthread
LDLIBS=-lcrypt

all: tpsample bruteforce_main encrypt

tpsample: tpsample.o threadpool.o
bruteforce_main: bruteforce_main.o bruteforce.o threadpool.o
encrypt: encrypt.o

clean:
	-rm -rf tpsample bruteforce_main encrypt tpsample.o bruteforce_main.o bruteforce.o threadpool.o encrypt.o
tidy: clean
	-rm -rf *~

.PHONY: all clean tidy
