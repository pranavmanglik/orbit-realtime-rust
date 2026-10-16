# Orbit Realtime Rust

`orbit-realtime-rust` provides bounded Server-Sent Events framing and an HTTP body implementation for Rust services. It exposes the standard `http-body` contract so applications can use their chosen compatible HTTP server without adding a framework here.

```toml
[dependencies]
orbit-realtime-rust = "0.1.0-alpha.1"
futures-util = "0.3" # Used by the stream construction example below.
```

```rust
use futures_util::stream;
use orbit_realtime_rust::{Event, response};
use std::convert::Infallible;

let event = Event::new("updated")?.with_event("record.changed")?;
let response = response(stream::iter([Ok::<_, Infallible>(event)]))?;
```

Place the example in a function returning `Result<_, Box<dyn std::error::Error>>`. `response`
returns an `http::Response<SseBody<_>>`; adapt it to the body type accepted by the HTTP server you
already use. This crate intentionally does not choose an HTTP server or framework.

The body pulls one event per poll, preserving downstream backpressure without an internal queue. It sets `text/event-stream`, `no-cache, no-transform`, `nosniff`, and `X-Accel-Buffering: no` headers. Encoded events are bounded to 64 KiB. Body errors use a sanitized error type and discard source details; producers should record safe telemetry before returning errors. Event payloads and IDs are omitted from debug output. `with_event("")` emits an explicit empty event type, and `with_id("")` emits the empty ID field that resets browser reconnect state; this matches the Python and Go packages.

The package does not provide authentication, authorization, event retention, replay, shared fanout, heartbeat scheduling, or reconnect policy. Secure the route in the application and bound upstream subscriptions.

See [architecture](docs/architecture/overview.md), [operations](docs/operations/README.md), [security](docs/security/overview.md), and [development](docs/development/README.md).

```sh
cargo fmt --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo build --release --locked
```

This is a **v0.1.0-alpha review candidate**. On Rust 1.85.0 and stable, local formatting, all five tests, Clippy with warnings denied, and locked release builds pass. Hosted CI, proxy disconnect/flush interoperability, representative multi-instance load and soak tests, and release provenance remain open release gates. The alpha status is not a production certification. Licensed under Apache-2.0.
