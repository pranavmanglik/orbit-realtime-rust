# Architecture

This repository is the Rust language implementation of Orbit's provider-neutral SSE capability. `Event` owns bounded protocol framing; `SseBody` implements `http-body::Body` over a caller-owned pull-based stream.

The body requests one event when polled and has no internal queue. Source and framing failures end the body using sanitized errors that do not retain provider text. The response requests no-cache/no-transform behavior and disables buffering in common reverse proxies. Compatible HTTP runtimes still own socket writes and disconnect handling. `orbit-core` remains the Python application runtime; this package is a separately deployable Rust implementation for services that choose that language.

No Core plugin protocol, cross-process event protocol, persistence, broker integration, or event bus is implied by the SSE wire format. Authentication and route ownership remain with the host application.
