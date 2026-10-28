CC      ?= cc
CFLAGS  ?= -O2 -g
CFLAGS  += -std=c11 -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion \
           -D_GNU_SOURCE -Isrc -pthread
LDFLAGS += -pthread

BUILD   := build
COMMON  := $(BUILD)/hashtable.o $(BUILD)/buffer.o

.PHONY: all test unit clean

all: unit

$(BUILD):
	mkdir -p $@

$(BUILD)/%.o: src/%.c src/*.h | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/test_hashtable: tests/test_hashtable.c tests/test.h $(COMMON)
	$(CC) $(CFLAGS) -Itests $< $(COMMON) -o $@ $(LDFLAGS)

unit: $(BUILD)/test_hashtable
	$(BUILD)/test_hashtable

test: unit

clean:
	rm -rf $(BUILD)
