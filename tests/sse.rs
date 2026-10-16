// Copyright 2026-present Orbit Contributors.
// Licensed under the Apache License, Version 2.0.

use std::convert::Infallible;

use futures_util::stream;
use http_body_util::BodyExt;
use orbit_realtime_rust::{Event, MAX_EVENT_BYTES, SseBodyError, encode_comment, response};

#[tokio::test]
async fn encodes_events_and_sets_safe_headers() {
    let event = Event::new("first\r\nsecond")
        .expect("data")
        .with_event("change")
        .expect("event")
        .with_id("42")
        .expect("id")
        .with_retry(2500)
        .expect("retry");
    assert_eq!(
        event.encode().expect("encoded"),
        "event: change\nid: 42\nretry: 2500\ndata: first\ndata: second\n\n"
    );
    let body = response(stream::iter([Ok::<_, Infallible>(event)])).expect("response");
    assert_eq!(
        body.headers()["content-type"],
        "text/event-stream; charset=utf-8"
    );
    assert_eq!(body.headers()["cache-control"], "no-cache, no-transform");
    assert_eq!(body.headers()["x-accel-buffering"], "no");
    let bytes = body.into_body().collect().await.expect("body").to_bytes();
    assert!(
        std::str::from_utf8(&bytes)
            .expect("utf8")
            .contains("data: second")
    );
}

#[tokio::test]
async fn body_errors_hide_application_details_and_end_the_stream() {
    let body = response(stream::iter([Err::<Event, _>(std::io::Error::other(
        "private provider token",
    ))]))
    .expect("response")
    .into_body();
    let mut body = Box::pin(body);

    let frame = body.as_mut().frame().await.expect("one error frame");
    let error = frame.expect_err("source error");
    assert_eq!(error.to_string(), "SSE event source failed");
    assert!(!error.to_string().contains("private provider token"));
    assert!(body.as_mut().frame().await.is_none());
}

#[tokio::test]
async fn body_reports_bounded_encoding_errors_without_source_details() {
    let event = Event::new("x".repeat(MAX_EVENT_BYTES - 16))
        .expect("bounded input")
        .with_id("identifier")
        .expect("bounded id");
    let body = response(stream::iter([Ok::<_, Infallible>(event)]))
        .expect("response")
        .into_body();
    let mut body = Box::pin(body);

    let frame = body.as_mut().frame().await.expect("one error frame");
    let error = frame.expect_err("encoding error");
    assert!(matches!(error, SseBodyError::Encoding(_)));
    assert!(body.as_mut().frame().await.is_none());
}

#[test]
fn bounds_fields_and_hides_sensitive_debug_content() {
    assert!(
        Event::new("x".repeat(MAX_EVENT_BYTES))
            .expect("bounded input")
            .encode()
            .is_err()
    );
    assert!(
        Event::new("x")
            .expect("event")
            .with_event("bad\nevent")
            .is_err()
    );
    assert!(Event::new("x").expect("event").with_id("bad\0id").is_err());
    let event = Event::new("private-payload")
        .expect("event")
        .with_id("secret-id")
        .expect("id");
    let debug = format!("{event:?}");
    assert!(!debug.contains("private-payload"));
    assert!(!debug.contains("secret-id"));
    assert_eq!(
        encode_comment("keep\nalive").expect("comment"),
        ": keep\n: alive\n\n"
    );
}

#[test]
fn empty_optional_fields_match_python_and_go_contracts() {
    let event = Event::new("payload")
        .expect("event")
        .with_event("")
        .expect("empty event name")
        .with_id("")
        .expect("empty id");
    assert_eq!(
        event.encode().expect("encoded"),
        "event: \nid: \ndata: payload\n\n"
    );
}
