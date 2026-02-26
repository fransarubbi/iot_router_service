# Iot Router Service

IoT Router Service is a lightweight, high-performance Rust service that routes messages between IoT devices and 
backend systems. It is designed to act as a protocol bridge and message router, providing observability and resilience
for constrained IoT environments.


## Key features

- High-performance message routing written in Rust (low memory usage, safe concurrency)
- Protocol bridging
- Configurable routing rules and transformation pipelines
- mTLS support and authentication for external connections
- Structured logging and configurable log level
- Graceful shutdown and backpressure handling


## Architecture

At a high level, iot_router_service consists of:

- Router core:
  - Receives messages from transports
  - Applies routing rules
  - Sends messages to destination adapters
- Observability:
  - Structured logs (JSON)

The codebase is modular so you can add new adapters and transform plugins.

## Prerequisites

- Rust toolchain (stable) — install from https://rustup.rs
- Optional: Docker and docker-compose for containerized deployment

## Quickstart

Clone and build:

```bash
git clone https://github.com/fransarubbi/iot_router_service.git
cd iot_router_service
# build in release mode
cargo build --release
```


## Configuration

The .env.example file is an example of the parameters the system needs to run. Use it as a template to create your own .env file.

```env
# gRPC
GRPC_HOST=localhost
GRPC_PORT_EDGE=50050
GRPC_PORT_SERVER=50051

# Logging
RUST_LOG=info

# Otros
APP_NAME=iot_router_service
ENVIRONMENT=development

```
