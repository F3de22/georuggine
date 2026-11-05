Georuggine — Real-Time Vehicle Fleet Tracking System
![CI Pipeline](https://github.com/G37/G37/actions/workflows/ci.yml/badge.svg)
![Rust Edition](https://img.shields.io/badge/rust-2021-brightgreen.svg)
![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)
Georuggine is a high-performance, fault-tolerant Client/Server system written in Rust for real-time monitoring, state tracking, and bi-directional communication across a fleet of simulated vehicles.
The system architecture consists of an async multi-threaded server backed by SQLite persistence and a feature-rich terminal user interface (TUI) client built with Ratatui and Tokio.
---
🌟 Key Features
Real-Time Tracking & State Machine: Emulates GPS position updates every 30 seconds. The server tracks vehicle states (`Disconnected`, `Still`, `Moving`), automatically triggering state transitions when a vehicle moves > 5 meters or remains stationary for $\ge 3$ minutes (180s).
4 Flexible GPS Emulation Strategies: Supports pre-recorded CSV routes, constant-speed routes on spherical geodesics with scheduled pauses, bounded random walks with reproducible seeds, and manual operator input via TUI.
Resilient Network Engine: Implements exponential backoff (1s, 2s, 4s, 8s, up to 30s) with 20% random jitter to prevent thundering herd server overload upon reconnection. Features an offline ring-buffer queue holding up to 200 position updates.
Terminal User Interface (TUI): Multi-pane interface providing live vehicle telemetry, position history with displacement ($\Delta\text{m}$), bi-directional chat log, movement analytics report, and a command bar.
High Performance & Observability: Low CPU/memory footprint with periodic CPU usage logging (every 120s).
Strict Software Invariants: Zero panics (`unwrap`/`expect` free in production), no shared mutable state (`Arc<Mutex>`) across `.await` points, non-blocking asynchronous architecture, and injectable time via the `Clock` trait for microsecond-scale testing.
---
📁 Repository Structure
The project is structured as a Cargo workspace with distinct crates to ensure clean separation of concerns and independent testability:
```text
georuggine/
├── Cargo.toml                    # Workspace manifest
├── crates/
│   ├── common/                   # Shared domain types, protocol messages, codecs, Clock trait
│   ├── server/                   # Server library & binary (SQLite, state machine, CPU logger)
│   ├── client/                   # Client library & binary
│   │   ├── src/
│   │   │   ├── main.rs           # Thin executable entrypoint
│   │   │   ├── lib.rs            # Client library root
│   │   │   ├── core.rs           # Async network task, backoff engine, offline queue
│   │   │   ├── sources.rs        # PositionSource trait & 4 GPS simulation strategies
│   │   │   └── tui/              # Ratatui TUI (render, UiState, command parser)
│   │   └── assets/               # Sample CSV route files (e.g., percorso_demo.csv)
│   └── tests/                    # Integration test crate (28 comprehensive test suites)
```
---
🏗 System Architecture & Protocol
Network Framing & Protocol
Communication occurs over raw TCP streams formatted with a custom binary protocol:
$$\text{[ 4-byte Big-Endian Length Header ]} + \text{[ Postcard Serialized Payload (Max 1 MiB) ]}$$
```text
+-------------------+       Hello (Protocol v1)       +-------------------+
|                   | ------------------------------> |                   |
|                   | <------------------------------ |                   |
|                   |        Welcome (Protocol v1)    |                   |
|   Client Handle   |                                 |   Server Engine   |
|  (Tokio Network)  |   Login / Register {user, pass} |   (SQLite DB)     |
|                   | ------------------------------> |                   |
|                   | <------------------------------ |                   |
|                   |     SessionToken {token: 64B}   |                   |
+-------------------+                                 +-------------------+
          |                                                     |
          | --- Position {coord, timestamp} (every 30s) ------> | (Saved to DB)
          | <-- StateChanged {Moving | Still} ----------------- | (Evaluated)
          | <-- Broadcast / Direct Messages -------------------- | (Operator)
```
Client Concurrency & Channel Decoupling
The TUI interface and the network task operate on separate async tasks, communicating exclusively via two `tokio::sync::mpsc` channels with a capacity of 32 messages each:
```text
 +-------------------------------------------------------------------------+
 |                              Client Process                             |
 |                                                                         |
 |  +-----------------------+   ClientCommand (cap: 32)   +-------------+  |
 |  |     Ratatui TUI       | --------------------------> |   Network   |  |
 |  | (20 FPS Render Loop)  |                             | Async Task  |  |
 |  |                       | <-------------------------- |  (core.rs)  |  |
 |  +-----------------------+    ClientEvent (cap: 32)    +-------------+  |
 |              |                                                |         |
 |   spawn_blocking (Crossterm)                           TCP Stream       |
 +--------------|------------------------------------------------|---------+
                v                                                v
       Terminal Keyboard                                    TCP Socket
```
---
🛠 Position Source Strategies (`sources.rs`)
All movement strategies implement the `PositionSource` trait:
`FileSource` (`--source file`): Parses CSV files (`lat,lon,offset_secs`). Anchor time zero is set on the first call to `next_position()`, ensuring authentication delay does not consume route steps. Supports optional looping (`--file-loop`).
`RandomWalkSource` (`--source random`): Generates step-by-step bounded movement using a deterministic seed (`StdRng`). Guaranteeing reproducible paths for testing and debugging.
`RouteSource` (`--source route`): Computes constant-speed movement between points using spherical trigonometry on geodesic great circles. Avoids linear degree-averaging errors near poles. Fuses overlapping scheduled pauses.
`ManualSource` (`--source manual`): Thread-safe synchronous `mpsc` channel allowing the operator to update vehicle coordinates dynamically using the `/pos <lat> <lon>` TUI command.
---
🚀 Getting Started
Prerequisites
Rust Toolchain: `1.75.0` or higher (Cargo, `rustc`).
1. Start the Server
Run the server binary by providing an SQLite database file:
```bash
cargo run --bin georuggine-server -- --db demo.db
```
2. Start Client Instances
Open separate terminal sessions to simulate different vehicles using any of the four strategies:
File Route (with New User Registration):
```bash
  cargo run --bin georuggine-client -- \
    --server 127.0.0.1:7878 \
    --user furgone \
    --password abc \
    --register \
    --source file \
    --file-path crates/client/assets/percorso_demo.csv
  ```
Constant Speed Route (with Pauses):
```bash
  cargo run --bin georuggine-client -- \
    --server 127.0.0.1:7878 \
    --user camion \
    --password abc \
    --source route \
    --start 45.07,7.68 \
    --end 45.46,9.19 \
    --speed 60 \
    --pause 0.5@120
  ```
Manual Position Control:
```bash
  cargo run --bin georuggine-client -- \
    --server 127.0.0.1:7878 \
    --user auto \
    --password abc \
    --source manual \
    --start 45.0703,7.6869
  ```
Random Walk (Reproducible Seed):
```bash
  cargo run --bin georuggine-client -- \
    --server 127.0.0.1:7878 \
    --user patrol_01 \
    --password abc \
    --source random \
    --start 45.07,7.68 \
    --radius 500 \
    --step 20 \
    --seed 42
  ```
---
🎮 Interactive TUI Commands
While inside the client application, type any of the following commands in the bottom input bar:
`/msg <text>` — Sends a text message to the server operator.
`/pos <lat> <lon>` — Updates coordinates manually (only for `--source manual`).
`/analisi <today|week|month>` — Requests route analytics (distance, average speed, moving time, pause time, stop counts).
`/quit` or `Ctrl+C` — Gracefully closes the session, sends `Logout`, and exits.
---
🧪 Testing & Verification
The repository includes over 200 unit tests and 28 integration tests:
```bash
# Run the entire test suite across all workspace crates
cargo test --workspace

# Run integration tests specifically
cargo test --test '*' --workspace

# Check code formatting compliance
cargo fmt --check

# Run Clippy lints with strict warning enforcement
cargo clippy --workspace -- -D warnings
```
Integration Test Architecture
The integration test suite (`crates/tests`) launches real in-memory server instances on system-assigned ephemeral ports (`127.0.0.1:0`) and drives frame-by-frame interactions using a `FakeClock`. This allows verifying 3-minute vehicle state transitions and network failures deterministically in microsecond execution times.
---
📜 License
Distributed under the MIT License. See `LICENSE` for details.
