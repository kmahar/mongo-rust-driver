pub mod auth;
#[cfg(feature = "csfle")]
pub(crate) mod csfle;
mod executor;
pub mod options;
pub mod session;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use derivative::Derivative;

#[cfg(test)]
use crate::options::ServerAddress;
use crate::{
    bson::Document,
    change_stream::{
        event::ChangeStreamEvent,
        options::ChangeStreamOptions,
        session::SessionChangeStream,
        ChangeStream,
    },
    concern::{ReadConcern, WriteConcern},
    db::Database,
    error::{ErrorKind, Result},
    event::command::CommandEventHandler,
    operation::{AggregateTarget, ListDatabases},
    options::{
        ClientOptions,
        DatabaseOptions,
        ListDatabasesOptions,
        ReadPreference,
        SelectionCriteria,
        SessionOptions,
    },
    results::DatabaseSpecification,
    sdam::{server_selection, SelectedServer, SessionSupportStatus, Topology},
    ClientSession,
};
pub(crate) use executor::{HELLO_COMMAND_NAMES, REDACTED_COMMANDS};
pub(crate) use session::{ClusterTime, SESSIONS_UNSUPPORTED_COMMANDS};

use session::{ServerSession, ServerSessionPool};

const DEFAULT_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// This is the main entry point for the API. A `Client` is used to connect to a MongoDB cluster.
/// By default, it will monitor the topology of the cluster, keeping track of any changes, such
/// as servers being added or removed.
///
/// `Client` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads or async tasks. For example:
///
/// ```rust
/// # #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
/// # use mongodb::{bson::Document, Client, error::Result};
/// # #[cfg(feature = "async-std-runtime")]
/// # use async_std::task;
/// # #[cfg(feature = "tokio-runtime")]
/// # use tokio::task;
/// #
/// # #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
/// # async fn start_workers() -> Result<()> {
/// let client = Client::with_uri_str("mongodb://example.com").await?;
///
/// for i in 0..5 {
///     let client_ref = client.clone();
///
///     task::spawn(async move {
///         let collection = client_ref.database("items").collection::<Document>(&format!("coll{}", i));
///
///         // Do something with the collection
///     });
/// }
/// #
/// # Ok(())
/// # }
/// ```
/// ## Notes on performance
/// Spawning many asynchronous tasks that use the driver concurrently like this is often the best
/// way to achieve maximum performance, as the driver is designed to work well in such situations.
///
/// Additionally, using a custom Rust type that implements `Serialize` and `Deserialize` as the
/// generic parameter of [`Collection`](../struct.Collection.html) instead of [`bson::Document`] can
/// reduce the amount of time the driver and your application spends serializing and deserializing
/// BSON, which can also lead to increased performance.
///
/// ## TCP Keepalive
/// TCP keepalive is enabled by default with ``tcp_keepalive_time`` set to 120 seconds. The
/// driver does not set ``tcp_keepalive_intvl``. See the
/// [MongoDB Diagnostics FAQ keepalive section](https://www.mongodb.com/docs/manual/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments)
/// for instructions on setting these values at the system level.
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ClientInner {
    topology: Topology,
    options: ClientOptions,
    session_pool: ServerSessionPool,
    #[cfg(feature = "csfle")]
    csfle: tokio::sync::RwLock<Option<csfle::ClientState>>,
}

impl Client {
    /// Creates a new `Client` connected to the cluster specified by `uri`. `uri` must be a valid
    /// MongoDB connection string.
    ///
    /// See the documentation on
    /// [`ClientOptions::parse`](options/struct.ClientOptions.html#method.parse) for more details.
    pub async fn with_uri_str(uri: impl AsRef<str>) -> Result<Self> {
        let options = ClientOptions::parse_uri(uri.as_ref(), None).await?;

        Client::with_options(options)
    }

    /// Creates a new `Client` connected to the cluster specified by `options`.
    pub fn with_options(options: ClientOptions) -> Result<Self> {
        options.validate()?;

        let inner = Arc::new(ClientInner {
            topology: Topology::new(options.clone())?,
            session_pool: ServerSessionPool::new(),
            #[cfg(feature = "csfle")]
            csfle: Default::default(),
            options,
        });
        Ok(Self { inner })
    }

    /// Creates a new `Client` connected to the cluster specified by `options` with auto-encryption
    /// enabled.
    #[cfg(feature = "csfle")]
    pub async fn with_encryption_options(
        options: ClientOptions,
        auto_enc: options::AutoEncryptionOptions,
    ) -> Result<Self> {
        let client = Self::with_options(options)?;
        *client.inner.csfle.write().await = Some(csfle::ClientState::new(&client, auto_enc).await?);
        Ok(client)
    }

    pub(crate) fn emit_command_event(&self, emit: impl FnOnce(&Arc<dyn CommandEventHandler>)) {
        if let Some(ref handler) = self.inner.options.command_event_handler {
            emit(handler);
        }
    }

    /// Gets the default selection criteria the `Client` uses for operations..
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.options.selection_criteria.as_ref()
    }

    /// Gets the default read concern the `Client` uses for operations.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.options.read_concern.as_ref()
    }

    /// Gets the default write concern the `Client` uses for operations.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.options.write_concern.as_ref()
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// The `Database` options (e.g. read preference and write concern) will default to those of the
    /// `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database(&self, name: &str) -> Database {
        Database::new(self.clone(), name, None)
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// Operations done with this `Database` will use the options specified by `options` by default
    /// and will otherwise default to those of the `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database_with_options(&self, name: &str, options: DatabaseOptions) -> Database {
        Database::new(self.clone(), name, Some(options))
    }

    /// Gets a handle to the default database specified in the `ClientOptions` or MongoDB connection
    /// string used to construct this `Client`.
    ///
    /// If no default database was specified, `None` will be returned.
    pub fn default_database(&self) -> Option<Database> {
        self.inner
            .options
            .default_database
            .as_ref()
            .map(|db_name| self.database(db_name))
    }

    async fn list_databases_common(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListDatabasesOptions>>,
        session: Option<&mut ClientSession>,
    ) -> Result<Vec<DatabaseSpecification>> {
        let op = ListDatabases::new(filter.into(), false, options.into());
        self.execute_operation(op, session).await.and_then(|dbs| {
            dbs.into_iter()
                .map(|db_spec| {
                    bson::from_slice(db_spec.as_bytes()).map_err(crate::error::Error::from)
                })
                .collect()
        })
    }

    /// Gets information about each database present in the cluster the Client is connected to.
    pub async fn list_databases(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListDatabasesOptions>>,
    ) -> Result<Vec<DatabaseSpecification>> {
        self.list_databases_common(filter, options, None).await
    }

    /// Gets information about each database present in the cluster the Client is connected to
    /// using the provided `ClientSession`.
    pub async fn list_databases_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListDatabasesOptions>>,
        session: &mut ClientSession,
    ) -> Result<Vec<DatabaseSpecification>> {
        self.list_databases_common(filter, options, Some(session))
            .await
    }

    /// Gets the names of the databases present in the cluster the Client is connected to.
    pub async fn list_database_names(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListDatabasesOptions>>,
    ) -> Result<Vec<String>> {
        let op = ListDatabases::new(filter.into(), true, options.into());
        match self.execute_operation(op, None).await {
            Ok(databases) => databases
                .into_iter()
                .map(|doc| {
                    let name = doc
                        .get_str("name")
                        .map_err(|_| ErrorKind::InvalidResponse {
                            message: "Expected \"name\" field in server response, but it was not \
                                      found"
                                .to_string(),
                        })?;
                    Ok(name.to_string())
                })
                .collect(),
            Err(e) => Err(e),
        }
    }

    /// Starts a new `ClientSession`.
    pub async fn start_session(
        &self,
        options: impl Into<Option<SessionOptions>>,
    ) -> Result<ClientSession> {
        let options = options.into();
        if let Some(ref options) = options {
            options.validate()?;
        }
        match self.get_session_support_status().await? {
            SessionSupportStatus::Supported {
                logical_session_timeout,
            } => Ok(self
                .start_session_with_timeout(logical_session_timeout, options, false)
                .await),
            _ => Err(ErrorKind::SessionsNotSupported.into()),
        }
    }

    /// Starts a new [`ChangeStream`] that receives events for all changes in the cluster. The
    /// stream does not observe changes from system collections or the "config", "local" or
    /// "admin" databases. Note that this method (`watch` on a cluster) is only supported in
    /// MongoDB 4.0 or greater.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/changeStreams/) on change
    /// streams.
    ///
    /// Change streams require either a "majority" read concern or no read
    /// concern. Anything else will cause a server error.
    ///
    /// Note that using a `$project` stage to remove any of the `_id` `operationType` or `ns` fields
    /// will cause an error. The driver requires these fields to support resumability. For
    /// more information on resumability, see the documentation for
    /// [`ChangeStream`](change_stream/struct.ChangeStream.html)
    ///
    /// If the pipeline alters the structure of the returned events, the parsed type will need to be
    /// changed via [`ChangeStream::with_type`].
    pub async fn watch(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<ChangeStreamOptions>>,
    ) -> Result<ChangeStream<ChangeStreamEvent<Document>>> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);
        options
            .get_or_insert_with(Default::default)
            .all_changes_for_cluster = Some(true);
        let target = AggregateTarget::Database("admin".to_string());
        self.execute_watch(pipeline, options, target, None).await
    }

    /// Starts a new [`SessionChangeStream`] that receives events for all changes in the cluster
    /// using the provided [`ClientSession`].  See [`Client::watch`] for more information.
    pub async fn watch_with_session(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<ChangeStreamOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionChangeStream<ChangeStreamEvent<Document>>> {
        let mut options = options.into();
        resolve_read_concern_with_session!(self, options, Some(&mut *session))?;
        resolve_selection_criteria_with_session!(self, options, Some(&mut *session))?;
        options
            .get_or_insert_with(Default::default)
            .all_changes_for_cluster = Some(true);
        let target = AggregateTarget::Database("admin".to_string());
        self.execute_watch_with_session(pipeline, options, target, None, session)
            .await
    }

    /// Check in a server session to the server session pool.
    /// If the session is expired or dirty, or the topology no longer supports sessions, the session
    /// will be discarded.
    pub(crate) async fn check_in_server_session(&self, session: ServerSession) {
        let session_support_status = self.inner.topology.session_support_status();
        if let SessionSupportStatus::Supported {
            logical_session_timeout,
        } = session_support_status
        {
            self.inner
                .session_pool
                .check_in(session, logical_session_timeout)
                .await;
        }
    }

    /// Starts a `ClientSession`.
    ///
    /// This method will attempt to re-use server sessions from the pool which are not about to
    /// expire according to the provided logical session timeout. If no such sessions are
    /// available, a new one will be created.
    pub(crate) async fn start_session_with_timeout(
        &self,
        logical_session_timeout: Option<Duration>,
        options: Option<SessionOptions>,
        is_implicit: bool,
    ) -> ClientSession {
        ClientSession::new(
            self.inner
                .session_pool
                .check_out(logical_session_timeout)
                .await,
            self.clone(),
            options,
            is_implicit,
        )
    }

    #[cfg(test)]
    pub(crate) async fn clear_session_pool(&self) {
        self.inner.session_pool.clear().await;
    }

    #[cfg(test)]
    pub(crate) async fn is_session_checked_in(&self, id: &Document) -> bool {
        self.inner.session_pool.contains(id).await
    }

    /// Get the address of the server selected according to the given criteria.
    /// This method is only used in tests.
    #[cfg(test)]
    pub(crate) async fn test_select_server(
        &self,
        criteria: Option<&SelectionCriteria>,
    ) -> Result<ServerAddress> {
        let server = self.select_server(criteria).await?;
        Ok(server.address.clone())
    }

    /// Select a server using the provided criteria. If none is provided, a primary read preference
    /// will be used instead.
    async fn select_server(&self, criteria: Option<&SelectionCriteria>) -> Result<SelectedServer> {
        let criteria =
            criteria.unwrap_or(&SelectionCriteria::ReadPreference(ReadPreference::Primary));

        let start_time = Instant::now();
        let timeout = self
            .inner
            .options
            .server_selection_timeout
            .unwrap_or(DEFAULT_SERVER_SELECTION_TIMEOUT);

        let mut watcher = self.inner.topology.watch();
        loop {
            let state = watcher.observe_latest();

            if let Some(server) = server_selection::attempt_to_select_server(
                criteria,
                &state.description,
                &state.servers(),
            )? {
                return Ok(server);
            }

            watcher.request_immediate_check();

            let change_occurred = start_time.elapsed() < timeout
                && watcher
                    .wait_for_update(timeout - start_time.elapsed())
                    .await;
            if !change_occurred {
                return Err(ErrorKind::ServerSelection {
                    message: state
                        .description
                        .server_selection_timeout_error_message(criteria),
                }
                .into());
            }
        }
    }

    #[cfg(all(test, not(feature = "sync"), not(feature = "tokio-sync")))]
    pub(crate) fn get_hosts(&self) -> Vec<String> {
        let watcher = self.inner.topology.watch();
        let state = watcher.peek_latest();

        state
            .servers()
            .keys()
            .map(|stream_address| format!("{}", stream_address))
            .collect()
    }

    #[cfg(test)]
    pub(crate) async fn sync_workers(&self) {
        self.inner.topology.sync_workers().await;
    }

    #[cfg(test)]
    pub(crate) fn topology_description(&self) -> crate::sdam::TopologyDescription {
        self.inner
            .topology
            .watch()
            .peek_latest()
            .description
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn topology(&self) -> &crate::sdam::Topology {
        &self.inner.topology
    }

    #[cfg(feature = "csfle")]
    pub(crate) fn weak(&self) -> WeakClient {
        WeakClient {
            inner: Arc::downgrade(&self.inner),
        }
    }

    #[cfg(feature = "csfle")]
    #[allow(dead_code)]
    pub(crate) async fn auto_encryption_opts(
        &self,
    ) -> Option<tokio::sync::RwLockReadGuard<'_, options::AutoEncryptionOptions>> {
        tokio::sync::RwLockReadGuard::try_map(self.inner.csfle.read().await, |csfle| {
            csfle.as_ref().map(|cs| cs.opts())
        })
        .ok()
    }
}

#[cfg(feature = "csfle")]
#[derive(Clone, Debug)]
pub(crate) struct WeakClient {
    inner: std::sync::Weak<ClientInner>,
}

#[cfg(feature = "csfle")]
impl WeakClient {
    #[allow(dead_code)]
    pub(crate) fn upgrade(&self) -> Option<Client> {
        self.inner.upgrade().map(|inner| Client { inner })
    }
}
