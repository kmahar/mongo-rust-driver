use crate::{
    bson::doc,
    client::options::ClientOptions,
    coll::options::FindOptions,
    test::{
        run_spec_test_with_path,
        spec::run_unified_format_test,
        TestClient,
        TracingEvent,
        DEFAULT_GLOBAL_TRACING_HANDLER,
        LOCK,
    },
    trace::{
        truncate_on_char_boundary,
        COMMAND_TRACING_EVENT_TARGET,
        DEFAULT_MAX_DOCUMENT_LENGTH_BYTES,
    },
};
use std::{iter, time::Duration};

#[test]
fn tracing_truncation() {
    let single_emoji = String::from("🤔");
    let two_emoji = String::from("🤔🤔");

    let mut s = two_emoji.clone();
    assert_eq!(s.len(), 8);

    // start of string is a boundary, so we should truncate there
    truncate_on_char_boundary(&mut s, 0);
    assert_eq!(s, String::from(""));

    // we should "round up" to the end of the first emoji
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 1);
    assert_eq!(s, single_emoji);

    // 4 is a boundary, so we should truncate there
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 4);
    assert_eq!(s, single_emoji);

    // we should round up to the full string
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 5);
    assert_eq!(s, two_emoji);

    // end of string is a boundary, so we should truncate there
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 8);
    assert_eq!(s, two_emoji);

    // we should get the full string back if the new length is longer than the original
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 10);
    assert_eq!(s, two_emoji);
}

fn debug_commands_filter(event: &TracingEvent) -> bool {
    event.target == COMMAND_TRACING_EVENT_TARGET && event.level >= tracing::Level::DEBUG
}

/// Prose test 1: Default truncation limit
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_logging_truncation_default_limit() {
    let _guard = LOCK.run_exclusively().await;
    let client = TestClient::new().await;
    let coll = client.init_db_and_coll("tracing_test", "truncation").await;

    let mut tracing_subscriber = DEFAULT_GLOBAL_TRACING_HANDLER.subscribe(debug_commands_filter);

    let docs = iter::repeat(doc! { "x": "y" }).take(100);
    coll.insert_many(docs, None)
        .await
        .expect("insert many should succeed");

    let events = tracing_subscriber
        .collect_events(Duration::from_millis(500), |_| true)
        .await;
    assert_eq!(events.len(), 2);

    let started = &events[0];
    let command = started.get_value_as_string("command");
    assert_eq!(command.len(), DEFAULT_MAX_DOCUMENT_LENGTH_BYTES);

    let succeeded = &events[1];
    let reply = succeeded.get_value_as_string("reply");
    assert!(reply.len() <= DEFAULT_MAX_DOCUMENT_LENGTH_BYTES);

    coll.find(None, None).await.expect("find should succeed");
    let succeeded = tracing_subscriber
        .wait_for_event(Duration::from_millis(500), |e| {
            e.get_value_as_string("message") == "Command succeeded"
        })
        .await
        .unwrap();
    let reply = succeeded.get_value_as_string("reply");
    assert_eq!(reply.len(), DEFAULT_MAX_DOCUMENT_LENGTH_BYTES);
}

/// Prose test 2: explicitly configured truncation limit
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_logging_truncation_explicit_limit() {
    let _guard = LOCK.run_exclusively().await;

    let client_opts = ClientOptions::builder()
        .tracing_max_document_length_bytes(5)
        .build();
    let client = TestClient::with_options(Some(client_opts)).await;

    let mut tracing_subscriber = DEFAULT_GLOBAL_TRACING_HANDLER.subscribe(debug_commands_filter);

    client
        .database("tracing_test")
        .run_command(doc! { "hello" : "true" }, None)
        .await
        .expect("hello command should succeed");

    let events = tracing_subscriber
        .collect_events(Duration::from_millis(500), |_| true)
        .await;
    assert_eq!(events.len(), 2);

    let started = &events[0];
    let command = started.get_value_as_string("command");
    assert_eq!(command.len(), 5);

    let succeeded = &events[1];
    let reply = succeeded.get_value_as_string("reply");
    assert_eq!(reply.len(), 5);

    // TODO RUST-1405: when we expose the full server reply for command errors, we should confirm
    // that gets correctly truncated in command failed events here as well.
}

/// Prose test 3: mid-codepoint truncation
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_logging_truncation_mid_codepoint_reply() {
    let _guard = LOCK.run_exclusively().await;

    let client_opts = ClientOptions::builder()
        .tracing_max_document_length_bytes(215)
        .build();
    let client = TestClient::with_options(Some(client_opts)).await;
    let coll = client.init_db_and_coll("tracing_test", "truncation").await;

    let mut tracing_subscriber = DEFAULT_GLOBAL_TRACING_HANDLER.subscribe(debug_commands_filter);

    let docs = iter::repeat(doc! { "🤔": "🤔🤔🤔🤔🤔🤔" }).take(10);
    coll.insert_many(docs, None)
        .await
        .expect("insert many should succeed");

    let started = tracing_subscriber
        .wait_for_event(Duration::from_millis(500), |e| {
            e.get_value_as_string("message") == "Command started"
        })
        .await
        .unwrap();

    let command = started.get_value_as_string("command");
    // 215 falls in the middle of an emoji (each is 4 bytes), so we should round up to 218.
    assert_eq!(command.len(), 218);

    let find_options = FindOptions::builder()
        .projection(doc! { "_id": 0, "🤔": 1 })
        .build();
    coll.find(None, find_options)
        .await
        .expect("find should succeed");
    let succeeded = tracing_subscriber
        .wait_for_event(Duration::from_millis(500), |e| {
            e.get_value_as_string("message") == "Command succeeded"
                && e.get_value_as_string("command_name") == "find"
        })
        .await
        .unwrap();
    let reply = succeeded.get_value_as_string("reply");
    // 215 falls in the middle of an emoji (each is 4 bytes), so we should round up to 218.
    assert_eq!(reply.len(), 218);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_logging_unified() {
    let _guard = LOCK.run_exclusively().await;
    run_spec_test_with_path(
        &["command-logging-and-monitoring", "logging", "unified"],
        run_unified_format_test,
    )
    .await;
}

// #[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
// #[cfg_attr(feature = "async-std-runtime", async_std::test)]
// async fn connection_logging_unified() {
//     let _guard = LOCK.run_exclusively().await;
//     run_spec_test_with_path(
//         &["connection-monitoring-and-pooling", "unified"],
//         run_unified_format_test,
//     )
//     .await;
// }
