CC      ?= cc
CFLAGS  ?= -O2 -g
CFLAGS  += -std=c11 -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion \
           -D_GNU_SOURCE -Isrc -pthread
LDFLAGS += -pthread

BUILD   := build
COMMON  := $(BUILD)/hashtable.o $(BUILD)/buffer.o $(BUILD)/protocol.o $(BUILD)/net.o
UNITS   := $(BUILD)/test_hashtable $(BUILD)/test_protocol $(BUILD)/test_threadpool

.PHONY: all unit test valgrind tsan bench clean

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

valgrind: all $(UNITS)
	tests/valgrind_check.sh

# ThreadSanitizer build of everything multi-threaded, then the same tests.
TSAN_DIR := $(BUILD)/tsan
TSAN_CC   = $(CC) -std=c11 -D_GNU_SOURCE -O1 -g -fsanitize=thread -Isrc -Itests -pthread
TSAN_LIB := src/hashtable.c src/buffer.c src/protocol.c src/net.c

tsan: | $(BUILD)
	mkdir -p $(TSAN_DIR)
	$(TSAN_CC) src/server.c src/threadpool.c $(TSAN_LIB) -o $(TSAN_DIR)/server
	$(TSAN_CC) src/epoll_server.c $(TSAN_LIB) -o $(TSAN_DIR)/epoll_server
	$(TSAN_CC) tests/test_hashtable.c $(TSAN_LIB) -o $(TSAN_DIR)/test_hashtable
	$(TSAN_CC) tests/test_threadpool.c src/threadpool.c -o $(TSAN_DIR)/test_threadpool
	TSAN_OPTIONS=halt_on_error=1 $(TSAN_DIR)/test_hashtable
	TSAN_OPTIONS=halt_on_error=1 $(TSAN_DIR)/test_threadpool
	TSAN_OPTIONS=halt_on_error=1 python3 tests/functional_test.py --server $(TSAN_DIR)/server
	TSAN_OPTIONS=halt_on_error=1 python3 tests/functional_test.py --server $(TSAN_DIR)/epoll_server --extra=-t4

bench: all
	python3 bench/run_bench.py

clean:
	rm -rf $(BUILD) server epoll_server client
