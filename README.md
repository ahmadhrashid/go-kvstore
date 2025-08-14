# Redis Clone (Go)

A Redis-compatible in-memory key–value server implemented in **Go**.
This project reimplements a useful subset of Redis functionality: networking, concurrency, blocking reads, streams, transactions, leader–follower replication and pub/sub to demonstrate systems and distributed-systems primitives. The server speaks the RESP protocol so it can be exercised with standard Redis tooling (e.g. `redis-cli`).

---

## Table of contents

* [Status / Scope](#status--scope)
* [Highlights / Features](#highlights--features)
* [Files & Architecture](#files--architecture)
* [Supported Commands (summary)](#supported-commands-summary)
* [Build & run](#build--run)
* [Examples & usage](#examples--usage)

  * [Basic commands](#basic-commands)
  * [Blocking lists / streams](#blocking-lists--streams)
  * [Replication example](#replication-example)
  * [Pub/Sub example (`SUBSCRIBE` / `PUBLISH` / `UNSUBSCRIBE`)](#pubsub-example-subscribe--publish--unsubscribe)
* [Design & internals](#design--internals)
* [Concurrency & correctness notes](#concurrency--correctness-notes)
* [Limitations & TODOs](#limitations--todos)
* [Testing & debugging tips](#testing--debugging-tips)
* [Contact](#contact)

---

## Status / Scope

**Status:** experimental / learning project. Not production-ready.

This repository aims to be interoperable with common Redis clients for many common commands and to showcase core distributed-systems concepts (replication, blocking reads, pub/sub) in a compact codebase.

---

## Highlights / Features

* RESP (Redis Serialization Protocol) parsing and encoding for client compatibility.
* TCP server implemented with Go `net` and per-connection goroutines for concurrency.
* In-memory data model:

  * Key–value store and TTL/expiry support (`SET` with `PX`).
  * Lists with blocking pop (`BLPOP`) semantics.
  * Streams with `XADD`, `XRANGE`, `XREAD` primitives and blocking semantics.
* Transactions via command queuing: `MULTI` / `EXEC` / `DISCARD`.
* Leader–follower replication primitives:

  * `PSYNC` / `REPLCONF` handshake, RDB snapshot transfer, per-replica ACK bookkeeping and `WAIT` for quorum acknowledgements.
* Pub/Sub:

  * `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE` with Redis-style RESP responses.
* Developer features:

  * Optional RDB load at startup (`--dir` + `--dbfilename`).
  * Command execution abstraction used to support transaction queuing.

---

## Files & Architecture

Top-level Go files and responsibilities:

* `main.go` - server bootstrap, connection accept loop, connection handler and command dispatch. Loads RDB (if provided) and starts listener.
* `replication.go` - PSYNC handshake, replica client logic (connect to master), ACK reader and replica bookkeeping, `WAIT` semantics.
* `transactions.go` - MULTI/EXEC/DISCARD and helper `execOne` logic for command execution.
* `handlers.go` - key/value commands (`SET`, `GET`, `CONFIG`, `INFO`, etc.).
* `lists.go` - list APIs (`LPUSH`, `RPUSH`, `LRANGE`, `LPOP`, `BLPOP`) plus blocked-client wakeup logic.
* `pubsub.go` - subscribe / unsubscribe / publish handling.
* Other helper types and structs are declared in the top-level files; everything is packaged together for clarity in this learning project.

---

## Supported Commands (summary)

> This is a concise view of commands implemented in the code base (behavior reflects code in repository).

**General**

* `PING`, `ECHO`, `INFO`, `CONFIG GET <key>`, `KEYS *`

**Strings / Keys**

* `SET key value [PX milliseconds]` - supports TTL via `PX`.
* `GET key`
* `INCR key`

**Lists**

* `RPUSH key value [value ...]`
* `LPUSH key value [value ...]`
* `LRANGE key start stop`
* `LLEN key`
* `LPOP key` (single and multi forms)
* `BLPOP key [key ...] timeout` - blocking pop (with timeout) and wakeup semantics

**Streams**

* `XADD` - add an entry to a stream
* `XRANGE` - read a range of entries
* `XREAD` - blocking read across streams

**Transactions**

* `MULTI`, `EXEC`, `DISCARD` - queueing and atomic execution of queued commands

**Replication**

* `REPLCONF`, `PSYNC`, `WAIT numreplicas timeout` - master/replica handshake and basic acknowledge-based waiting

**Pub/Sub**

* `SUBSCRIBE channel [channel ...]` - enter subscribe mode (server sends subscription confirmation messages)
* `UNSUBSCRIBE channel` - unsubscribe a client from a channel
* `PUBLISH channel message` - deliver message to all connected subscribers; returns number of subscribers that received the message

---

## Build & run

**Requirements**

* Go (1.18+ recommended)

**Build**

```bash
go build -o redis-clone .
```

**Run**

```bash
# default port 6379
./redis-clone --port 6379

# load an RDB file on start (if present)
./redis-clone --dir /path/to/rdbdir --dbfilename dump.rdb --port 6379

# run as a replica to connect to a master at 127.0.0.1:6379
./redis-clone --port 6380 --replicaof "127.0.0.1 6379"
```

Command-line flags:

* `--dir` - directory containing an RDB file to load at startup
* `--dbfilename` - RDB filename
* `--port` - listening port
* `--replicaof` - `"host port"` to connect to a master and act as a replica

---

## Examples & usage

You can interact with the server using `redis-cli` (recommended), or raw RESP via `nc`/`printf`.

### Basic commands

```bash
# Start server on port 6379
./redis-clone --port 6379

# In another shell:
redis-cli -p 6379 PING
# -> PONG

redis-cli -p 6379 SET mykey "hello"
redis-cli -p 6379 GET mykey
# -> "hello"

redis-cli -p 6379 INCR counter
# -> 1
```

### Blocking lists / streams

```bash
# In terminal A:
redis-cli -p 6379 BLPOP mylist 5
# waits up to 5 seconds for an element

# In terminal B:
redis-cli -p 6379 RPUSH mylist value1
# A will receive mylist/value1 and return immediately
```

### Replication example

1. Start master:

```bash
./redis-clone --port 6379
```

2. Start replica connecting to master:

```bash
./redis-clone --port 6380 --replicaof "127.0.0.1 6379"
```

Watch master / replica logs (stdout) to observe:

* REPLCONF/PSYNC handshake
* RDB snapshot transfer to replica
* Commands propagated from master to replica and per-replica ACK updates

Example: `WAIT 1 1000` on master will block until one replica acknowledges or until timeout.

### Pub/Sub example (`SUBSCRIBE` / `PUBLISH` / `UNSUBSCRIBE`)

**Subscribe**

Open a terminal and subscribe:

```bash
redis-cli -p 6379 SUBSCRIBE mychannel
```

This client enters subscribe mode. The server will reply with a subscription confirmation similar to:

```
1) "subscribe"
2) "mychannel"
3) (integer) 1
```

**Publish**

In another terminal:

```bash
redis-cli -p 6379 PUBLISH mychannel "hello world"
```

The publishing client will get an integer reply listing the number of subscribers that received the message (e.g., `:1`). The subscriber will receive the message in the format:

```
1) "message"
2) "mychannel"
3) "hello world"
```

**Unsubscribe**

From the subscribed client you can unsubscribe:

```
UNSUBSCRIBE mychannel
```

The server will respond with an unsubscribe confirmation similar to:

```
1) "unsubscribe"
2) "mychannel"
3) (integer) 0
```

**Notes about the Pub/Sub implementation**

* A client in subscribe mode receives only a limited set of allowed commands (e.g. `SUBSCRIBE`, `UNSUBSCRIBE`, `PING`, `QUIT`) - other commands will be rejected while in subscribe mode.
* Subscription state is stored per-connection in each client's `clientState.subscribed` map.
* `PUBLISH` iterates the global `clients` set, sends the message to each subscribed connection (RESP arrays matching Redis style), and returns the count of subscribers to the publisher.
* `UNSUBSCRIBE` removes the channel from the client's subscription set and returns the updated subscription count to the caller.

---

## Design & internals

### In-memory model

* `db map[string]string` - primary key/value store (strings)
* `expiryDB map[string]time.Time` - per-key TTL management
* `lists map[string][]string` - lists for list commands
* `streams map[string][]streamEntry` - stream entries
* `clients map[io.Writer]*clientState` - connected client state including `subscribed` set and subscribe-mode flag
* `replicaConns map[net.Conn]struct{}` - connected replica sockets
* `replicaOffsets map[net.Conn]int64` - per-replica offsets acknowledged
* `masterOffset int64` - master's running byte offset for propagated commands

### Command dispatch

* Incoming RESP arrays are parsed and dispatched in a `switch` statement in the connection handler.
* Many commands are implemented in dedicated handlers (e.g. `lists.go` for lists, `replication.go` for replication).
* `execOne` provides a reusable command execution path used by live commands and by queued transaction execution.

### Replication & `WAIT`

* `masterOffset` is incremented by the master when it propagates commands to replicas (size is based on encoded RESP command size).
* Replicas send `REPLCONF ACK <offset>` which is parsed by the master’s ACK reader goroutines and stored in `replicaOffsets`.
* `WAIT numreplicas timeout` inspects `replicaOffsets` to determine whether the requested quorum was reached. If not immediately met, the request is queued in `pendingWaits` and fulfilled when enough replicas ACK or when the timeout elapses.

---

## Concurrency & correctness notes

* The server uses goroutines per client connection and additional goroutines for replica ACK readers and timeout handling.
* Shared state for replication bookkeeping, stream/wait queues and blocked clients is protected by `sync.Mutex` instances in several places (`replicaConnsMu`, `waitingXReadsMu`, `blockedClientsMu`, `streamNotifiersMu`).
* **Caveat:** `clients` (the global client map) is mutated on connection setup/teardown and iterated during `PUBLISH`. Depending on workload and interleaving you may experience race conditions - consider protecting `clients` with a mutex or `sync.RWMutex` before using this implementation in a concurrent benchmark.
* Blocking operations are implemented with explicit request objects + timeout goroutines; these are carefully removed on connection close to avoid leaking state.

---

## Limitations & TODOs

**Current limitations**

* Not production-ready: no authentication, no ACLs, no TLS, limited error handling for some malformed RESP edge cases.
* `PUBLISH` performs synchronous delivery by iterating `clients`; there is no message queue, backpressure handling, or persistence for pub/sub.
* RDB persistence writing (snapshotting) and AOF are not implemented (only RDB load at startup is supported).
* No clustering, sharding, or automatic leader failover.
* Limited test coverage and benchmark data.

**Planned TODOs / improvements**

* Implement asynchronous or buffered `PUBLISH` delivery for higher throughput and backpressure safety.
* Add RDB save and/or AOF append-only log to persist writes.
* Add comprehensive unit tests and integration tests (commands, replication, pub/sub).
* Add metrics (command latency, per-command counters) and structured logging for observability.
* Implement `PSUBSCRIBE` / `PUNSUBSCRIBE` (pattern subscribe) support.

---

## Testing & debugging tips

* Use `redis-cli` for interactive testing.
* For raw RESP testing and automation, use `printf` & `nc`:

  ```bash
  # PING
  printf "*1\r\n$4\r\nPING\r\n" | nc 127.0.0.1 6379
  ```
* For `BLPOP` / `XREAD` tests, start one client blocking and another pushing data; check server stdout for wakeup logs.
* To debug replication: run master and replica with logs printed to stdout and trace `PSYNC` / `REPLCONF` exchanges (logs are printed by the code).
* To measure concurrency: write a tiny Go program that opens N concurrent connections and issues commands; monitor server logs and OS-level resource usage (e.g., `top`, `netstat`, `ss`).
* If you add a mutex around `clients`, re-run `go test` / race detector: `go run -race` can help find accidental data races while iterating/mutating shared structures.

---

## Contact

**Author:** Ahmad Rashid \
**Contact:** `ahmad.rashid@mail.utoronto.ca`\
**GitHub:** `https://github.com/ahmadhrashid` \
**LinkedIn:** `https://linkedin.com/in/ahmadhrashid`

---
