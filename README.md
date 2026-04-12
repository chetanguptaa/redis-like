# Redis-Inspired In-Memory Data Store

A Redis-compatible, single-threaded in-memory datastore built with Bun.js and TypeScript, designed to explore systems-level concepts including event-driven architecture, persistence, replication, and high-throughput request handling.

## ⚡ Highlights

- 🚀 ~120k ops/sec baseline throughput (SET/GET)
- ⚡ ~400k ops/sec with pipelined requests (P=32)
- 📉 Sub-3ms P95 latency under concurrent load (300 clients)
- 🔁 Read replica support with asynchronous replication
- 💾 RDB-based persistence for snapshot recovery
- 📡 Pub/Sub and Streams for real-time messaging
- 🔐 Transactions with optimistic locking
- 🌍 Supports Redis Serialization Protocol (RESP)

## 🧠 Architecture

The system is designed as a **single-threaded event-driven server**, similar to Redis, optimized for low-latency request handling.

### Core Components

- **Event Loop**: Handles all client connections and command execution in a non-blocking manner
- **RESP Parser**: Custom implementation for decoding/encoding Redis protocol messages
- **Command Dispatcher**: Routes parsed commands to appropriate handlers
- **In-Memory Store**: Optimized data structures for O(1) average-case operations
- **Persistence Layer**: RDB snapshotting for durability
- **Replication Module**: Asynchronous replication to read replicas

## 🧩 Features

### Core Data Structures
- Strings, Sorted Sets, Streams
- Expiry support with TTL-based eviction

### Messaging & Streaming
- Pub/Sub with channel-based fan-out
- Stream support for append-only event logs

### Transactions & Concurrency
- MULTI/EXEC transactions
- Optimistic locking (WATCH)

### Persistence
- RDB snapshotting for recovery

### Replication
- Read replicas with async synchronization

### Protocol
- Full RESP protocol support for compatibility with Redis clients

## 📊 Benchmark Results

Benchmarked using `redis-benchmark` on a local machine.

### Baseline Throughput

| Concurrency | SET (ops/sec) | GET (ops/sec) | Avg Latency |
|-------------|---------------|---------------|-------------|
| 10          | ~102k         | ~108k         | ~0.08 ms    |
| 50          | ~113k         | ~127k         | ~0.4 ms     |
| 100         | ~115k         | ~128k         | ~0.8 ms     |
| 300         | ~112k         | ~121k         | ~2.5 ms     |

---

### Pipelined Performance

| Pipeline Depth | Throughput (ops/sec) |
|----------------|----------------------|
| 16             | ~325k – 361k         |
| 32             | ~370k – 408k         |

---

### Observations

- Throughput remains stable under increasing concurrency
- Latency increases gradually without sharp degradation
- Pipelining provides ~3–4× throughput improvement

## ⚖️ Design Decisions & Tradeoffs

- **Single-threaded architecture**
  - Simplifies concurrency model
  - Avoids locking overhead
  - Trades off multi-core utilization

- **RDB persistence**
  - Faster snapshots
  - Risk of data loss between snapshots

- **Pipelining support**
  - Improves throughput significantly
  - Increases per-request latency due to batching

## ▶️ Running the Server

```bash
bun install
bun run app/main.ts or bun build app/main.ts --outdir ./dist --minify --target bun && node dist/main.js

redis-benchmark -p 6379 -t set,get -n 100000 -c 50
