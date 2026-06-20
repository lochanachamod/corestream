# CoreStream

CoreStream is a custom-built, distributed, partition-tolerant event streaming engine designed from scratch. It serves as a high-performance, low-level alternative to systems like Apache Kafka or RabbitMQ, built to understand the absolute lowest levels of backend engineering.

## Architecture Highlights
- **Networking Protocol**: Custom layer-4 binary protocol built on raw TCP Sockets.
- **Consensus**: Raft Protocol implementation from scratch for leader election and data replication.
- **Storage Core**: Linux File I/O APIs with Memory-Mapped Files (mmap) for dense indexing and zero-copy data transmission.
- **Serialization**: Protocol Buffers (Protobuf) for zero-copy binary serialization.

## Project Phases
1. Core Networking & Foundation
2. Binary Serialization (Protobuf)
3. Memory-Mapped Storage Engine
4. Raft Distributed Consensus
5. Zero-Copy Consumer Streaming
6. Autonomous Agentic Telemetry Integration
