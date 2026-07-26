CC      ?= gcc
CFLAGS  ?= -std=c11 -O2 -Wall -Wextra -Wpedantic
LDLIBS   = -lm

sensor: sensor.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f sensor

.PHONY: clean
