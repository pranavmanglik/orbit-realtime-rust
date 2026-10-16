# Operations

Terminate public TLS at a trusted ingress or configure TLS in the host Rust service. The response sets no-cache/no-transform directives and `X-Accel-Buffering: no`; verify the actual ingress flushes `text/event-stream` responses promptly and honors the buffering request. Set idle and drain timeouts suited to long-lived requests. Drop the response body and upstream subscription when the client disconnects.

Size concurrent streams from memory, file descriptor, and upstream subscription budgets. This package has no internal queue, heartbeat scheduler, or retry policy; implement explicit bounded policies in the application where needed. Test disconnect and shutdown behavior through the actual HTTP runtime and proxy before deployment.
