use crate::event::{
    cmap::{
        CmapEventHandler,
        ConnectionCheckedInEvent,
        ConnectionCheckedOutEvent,
        ConnectionCheckoutFailedEvent,
        ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent,
        ConnectionClosedEvent,
        ConnectionClosedReason,
        ConnectionCreatedEvent,
        ConnectionReadyEvent,
        PoolClearedEvent,
        PoolClosedEvent,
        PoolCreatedEvent,
        PoolReadyEvent,
    },
    command::{
        CommandEventHandler,
        CommandFailedEvent,
        CommandStartedEvent,
        CommandSucceededEvent,
    },
    sdam::{
        SdamEventHandler,
        ServerDescriptionChangedEvent,
        ServerOpeningEvent,
        ServerClosedEvent,
        TopologyDescriptionChangedEvent,
        TopologyOpeningEvent,
        TopologyClosedEvent,
        ServerHeartbeatStartedEvent,
        ServerHeartbeatSucceededEvent,
        ServerHeartbeatFailedEvent,
    }
};
use bson::Bson;

pub(crate) const COMMAND_TRACING_EVENT_TARGET: &str = "mongodb::command";
pub(crate) const CONNECTION_TRACING_EVENT_TARGET: &str = "mongodb::connection";
pub(crate) const SDAM_TRACING_EVENT_TARGET: &str = "mongodb::sdam";
pub(crate) const DEFAULT_MAX_DOCUMENT_LENGTH_BYTES: usize = 1000;

pub(crate) struct CommandTracingEventEmitter {
    max_document_length_bytes: usize,
    /// We always store this so we don't have to branch on whether or not we're in a test
    /// whenever we emit a tracing event, however it is only actually added to the events
    /// for tests.
    #[allow(dead_code)]
    client_id: Option<String>,
}

impl CommandTracingEventEmitter {
    pub(crate) fn new(
        max_document_length_bytes: Option<usize>,
        client_id: Option<String>,
    ) -> CommandTracingEventEmitter {
        CommandTracingEventEmitter {
            max_document_length_bytes: max_document_length_bytes
                .unwrap_or(DEFAULT_MAX_DOCUMENT_LENGTH_BYTES),
            client_id,
        }
    }
}

impl CommandEventHandler for CommandTracingEventEmitter {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        tracing_debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            command = serialize_command_or_reply(event.command, self.max_document_length_bytes).as_str(),
            database_name = event.db.as_str(),
            command_name = event.command_name.as_str(),
            request_id = event.request_id,
            driver_connection_id = event.connection.id,
            server_connection_id = event.connection.server_id,
            server_host = event.connection.address.host(),
            server_port = event.connection.address.port(),
            service_id = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            "Command started"
        );
    }

    fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
        tracing_debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            reply = serialize_command_or_reply(event.reply, self.max_document_length_bytes).as_str(),
            command_name = event.command_name.as_str(),
            request_id = event.request_id,
            driver_connection_id = event.connection.id,
            server_connection_id = event.connection.server_id,
            server_host = event.connection.address.host(),
            server_port = event.connection.address.port(),
            service_id = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            duration_ms = event.duration.as_millis(),
            "Command succeeded"
        );
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        let failure = match event.failure.kind.as_ref() {
            crate::error::ErrorKind::Redacted => None,
            _ => {
                Some(event.failure.tracing_representation())
            },
        };
        tracing_debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            failure = failure,
            command_name = event.command_name.as_str(),
            request_id = event.request_id,
            driver_connection_id = event.connection.id,
            server_connection_id = event.connection.server_id,
            server_host = event.connection.address.host(),
            server_port = event.connection.address.port(),
            service_id = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            duration_ms = event.duration.as_millis(),
            "Command failed"
        );
    }
}

pub(crate) struct ConnectionTracingEventEmitter {
    /// We always store this so we don't have to branch on whether or not we're in a test
    /// whenever we emit a tracing event, however it is only actually added to the events
    /// for tests.
    #[allow(dead_code)]
    client_id: Option<String>,
}

impl ConnectionTracingEventEmitter {
    pub(crate) fn new(client_id: Option<String>) -> ConnectionTracingEventEmitter {
        Self { client_id }
    }
}

impl CmapEventHandler for ConnectionTracingEventEmitter {
    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {
        let options_ref = event.options.as_ref();
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            max_idle_time_ms = options_ref.and_then(|o| o.max_idle_time.and_then(|m| Some(m.as_millis()))),
            max_pool_size = options_ref.and_then(|o| o.max_pool_size),
            min_pool_size = options_ref.and_then(|o| o.min_pool_size),
            "Connection pool created",
        );
    }

    fn handle_pool_ready_event(&self, event: PoolReadyEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            "Connection pool ready",
        );
    }

    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            service_id = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            "Connection pool cleared",
        );
    }

    fn handle_pool_closed_event(&self, event: PoolClosedEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            "Connection pool closed",
        );
    }

    fn handle_connection_created_event(&self, event: ConnectionCreatedEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            driver_connection_id = event.connection_id,
            "Connection created",
        );
    }

    fn handle_connection_ready_event(&self, event: ConnectionReadyEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            driver_connection_id = event.connection_id,
            "Connection ready",
        );
    }

    fn handle_connection_closed_event(&self, event: ConnectionClosedEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            driver_connection_id = event.connection_id,
            reason = event.reason.tracing_representation().as_str(),
            "Connection closed",
        );
    }

    fn handle_connection_checkout_started_event(&self, event: ConnectionCheckoutStartedEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            "Connection checkout started",
        );
    }

    fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            reason = event.reason.tracing_representation().as_str(),
            "Connection checkout failed",
        );
    }

    fn handle_connection_checked_out_event(&self, event: ConnectionCheckedOutEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            driver_connection_id = event.connection_id,
            "Connection checked out",
        );
    }

    fn handle_connection_checked_in_event(&self, event: ConnectionCheckedInEvent) {
        tracing_debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            driver_connection_id = event.connection_id,
            "Connection checked in",
        );
    }
}

pub(crate) struct SdamTracingEventEmitter {
    max_document_length_bytes: usize,
    /// We always store this so we don't have to branch on whether or not we're in a test
    /// whenever we emit a tracing event, however it is only actually added to the events
    /// for tests.
    #[allow(dead_code)]
    client_id: Option<String>,
}

impl SdamEventHandler for SdamTracingEventEmitter {
    fn handle_server_description_changed_event(&self, event: ServerDescriptionChangedEvent) {
        tracing_debug!(
            target: SDAM_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            topology_id = event.topology_id.tracing_representation(),
            previous_description = event.previous_description.tracing_representation(),
            new_description = event.new_description.tracing_representation(),
        );
    }

    fn handle_server_opening_event(&self, event: ServerOpeningEvent) {
        tracing_debug!(
            target: SDAM_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            topology_id = event.topology_id.tracing_representation(),
        );
    }

    fn handle_server_closed_event(&self, event: ServerClosedEvent) {
        tracing_debug!(
            target: SDAM_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.address.host(),
            server_port = event.address.port(),
            topology_id = event.topology_id.tracing_representation(),
        );
    }

    fn handle_topology_description_changed_event(&self, event: TopologyDescriptionChangedEvent) {
        tracing_debug!(
            target: SDAM_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            topology_id = event.topology_id.tracing_representation(),
            previous_description = event.previous_description.tracing_representation(),
            new_description = event.new_description.tracing_representation(),
        );
    }

    fn handle_topology_opening_event(&self, event: TopologyOpeningEvent) {
        tracing_debug!(
            target: SDAM_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            topology_id = event.topology_id.tracing_representation(),
        );
    }

    fn handle_topology_closed_event(&self, event: TopologyClosedEvent) {
        tracing_debug!(
            target: SDAM_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            topology_id = event.topology_id.tracing_representation(),
        );
    }

    fn handle_server_heartbeat_started_event(&self, event: ServerHeartbeatStartedEvent) {
        tracing_debug!(
            target: SDAM_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.server_address.host(),
            server_port = event.server_address.port(),
            awaited = false, // TODO: use actual value of `awaited`
        );
    }

    fn handle_server_heartbeat_succeeded_event(&self, event: ServerHeartbeatSucceededEvent) {
        tracing_debug!(
            target: SDAM_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.server_address.host(),
            server_port = event.server_address.port(),
            duration_ms = event.duration.as_millis(),
            reply = serialize_command_or_reply(event.reply, self.max_document_length_bytes),
            awaited = false, // TODO: use actual value of `awaited`
        );
    }

    fn handle_server_heartbeat_failed_event(&self, event: ServerHeartbeatFailedEvent) {
        tracing_debug!(
            target: SDAM_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            server_host = event.server_address.host(),
            server_port = event.server_address.port(),
            duration_ms = event.duration.as_millis(),
            //reply = serialize_command_or_reply(event.reply, self.max_document_length_bytes),
            awaited = false, // TODO: use actual value of `awaited`
        );
    }
}

trait TracingRepresentation {
    fn tracing_representation(self) -> String;
}

impl TracingRepresentation for bson::oid::ObjectId {
    fn tracing_representation(self) -> String {
        self.to_hex()
    }
}

impl TracingRepresentation for bson::Document {
    fn tracing_representation(self) -> String {
        Bson::Document(self).into_canonical_extjson().to_string()
    }
}

impl TracingRepresentation for crate::error::Error {
    fn tracing_representation(self) -> String {
        self.to_string()
    }
}

impl TracingRepresentation for ConnectionClosedReason {
    fn tracing_representation(self) -> String {
        match self {
            ConnectionClosedReason::Stale => {
                "Connection became stale because the pool was cleared".to_string()
            }
            ConnectionClosedReason::Idle => "Connection has been available but unused for longer \
                                             than the configured max idle time"
                .to_string(),
            ConnectionClosedReason::Error => {
                "An error occurred while using the connection".to_string()
            }
            ConnectionClosedReason::Dropped => {
                "Connection was dropped during an operation".to_string()
            }
            ConnectionClosedReason::PoolClosed => "Connection pool was closed".to_string(),
        }
    }
}

impl TracingRepresentation for ConnectionCheckoutFailedReason {
    fn tracing_representation(self) -> String {
        match self {
            ConnectionCheckoutFailedReason::Timeout => {
                "Wait queue timeout elapsed without a connection becoming available".to_string()
            }
            ConnectionCheckoutFailedReason::ConnectionError => {
                "An error occurred while trying to establish a connection".to_string()
            }
        }
    }
}

impl TracingRepresentation for crate::sdam::public::ServerInfo<'_> {
    fn tracing_representation(self) -> String {
        match bson::to_bson(&self) {
            Ok(bson) => bson.into_canonical_extjson().to_string(),
            Err(err) => format!("Failed to serialize server description: {}", err),
        }
    }
}

impl TracingRepresentation for crate::event::sdam::TopologyDescription {
    fn tracing_representation(self) -> String {
        match bson::to_bson(&self) {
            Ok(bson) => bson.into_canonical_extjson().to_string(),
            Err(err) => format!("Failed to serialize topology description: {}", err),
        }
    }
}

fn serialize_command_or_reply(doc: bson::Document, max_length_bytes: usize) -> String {
    let mut ext_json = doc.tracing_representation();
    truncate_on_char_boundary(&mut ext_json, max_length_bytes);
    ext_json
}

// TODO: subject to change based on what exact version of truncation we decide to go with.
/// Truncates the given string at the closest UTF-8 character boundary >= the provided length.
/// If the new length is >= the current length, does nothing.
pub(crate) fn truncate_on_char_boundary(s: &mut String, new_len: usize) {
    if s.len() > new_len {
        // to avoid generating invalid UTF-8, find the first index >= max_length_bytes that is
        // the end of a character.
        // TODO: eventually we should use ceil_char_boundary here but it's currently nightly-only.
        // see: https://doc.rust-lang.org/std/string/struct.String.html#method.ceil_char_boundary
        let mut truncate_index = new_len;
        // is_char_boundary returns true when the provided value == the length of the string, so
        // if we reach the end of the string this loop will terminate.
        while !s.is_char_boundary(truncate_index) {
            truncate_index += 1;
        }
        s.truncate(truncate_index);
    }
}

/// We don't currently use all of these levels but they are included for completeness.
#[allow(dead_code)]
pub(crate) enum TracingOrLogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl TracingOrLogLevel {
    pub(crate) const fn as_log_level(&self) -> log::Level {
        match self {
            TracingOrLogLevel::Error => log::Level::Error,
            TracingOrLogLevel::Warn => log::Level::Warn,
            TracingOrLogLevel::Info => log::Level::Info,
            TracingOrLogLevel::Debug => log::Level::Debug,
            TracingOrLogLevel::Trace => log::Level::Trace,
        }
    }

    pub(crate) const fn as_tracing_level(&self) -> tracing::Level {
        match self {
            TracingOrLogLevel::Error => tracing::Level::ERROR,
            TracingOrLogLevel::Warn => tracing::Level::WARN,
            TracingOrLogLevel::Info => tracing::Level::INFO,
            TracingOrLogLevel::Debug => tracing::Level::DEBUG,
            TracingOrLogLevel::Trace => tracing::Level::TRACE,
        }
    }
}

/// Emits a tracing event at debug level. In tests, the provided client ID will be added as field to
/// each event to enable distinguishing which client produced the event. Outside of tests, the ID
/// will be ignored.
macro_rules! tracing_debug {
    (target: $target:expr, client_id: $client_id:expr, $($field:tt)* ) => (
        #[cfg(test)]
        tracing::event!(target: $target, tracing::Level::DEBUG, { client_id = $client_id, $($field)* });
        #[cfg(not(test))]
        tracing::event!(target: $target, tracing::Level::DEBUG, { $($field)* });
    );
}
pub(crate) use tracing_debug;

macro_rules! trace_or_log_enabled {
    (target: $target:expr, $lvl:expr) => {
        tracing::enabled!(target: $target, $lvl.as_tracing_level())
            || log::log_enabled!(target: $target, $lvl.as_log_level())
    };
}
pub(crate) use trace_or_log_enabled;
