CC      ?= cc
CFLAGS  ?= -O2 -g
CFLAGS  += -std=c11 -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion \
           -D_GNU_SOURCE -Isrc -pthread
LDFLAGS += -pthread

BUILD   := build
COMMON  := $(BUILD)/hashtable.o $(BUILD)/buffer.o $(BUILD)/protocol.o $(BUILD)/net.o
UNITS   := $(BUILD)/test_hashtable $(BUILD)/test_protocol $(BUILD)/test_threadpool

.PHONY: all unit test clean

all: server epoll_server client

server: $(BUILD)/server.o $(BUILD)/threadpool.o $(COMMON)
	$(CC) $^ -o $@ $(LDFLAGS)

epoll_server: $(BUILD)/epoll_server.o $(COMMON)
	$(CC) $^ -o $@ $(LDFLAGS)

client: $(BUILD)/client.o $(BUILD)/net.o
	$(CC) $^ -o $@ $(LDFLAGS)

$(BUILD):
	mkdir -p $@

$(BUILD)/%.o: src/%.c src/*.h | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/test_%: tests/test_%.c tests/test.h $(COMMON) $(BUILD)/threadpool.o
	$(CC) $(CFLAGS) -Itests $< $(COMMON) $(BUILD)/threadpool.o -o $@ $(LDFLAGS)

unit: $(UNITS)
	@for t in $(UNITS); do ./$$t || exit 1; done

test: unit server epoll_server
	python3 tests/functional_test.py --server ./server
	python3 tests/functional_test.py --server ./epoll_server
	python3 tests/functional_test.py --server ./epoll_server --extra=-t4

clean:
	rm -rf $(BUILD) server epoll_server client
