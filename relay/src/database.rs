use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use nostr_database::nostr::JsonUtil;
use nostr_database::nostr::Kind;
use nostr_database::nostr::event::{Event, EventId};
use nostr_database::nostr::filter::Filter;
use nostr_database::nostr::util::BoxedFuture;
use nostr_database::{Backend, DatabaseError, Events, NostrDatabase, SaveEventStatus};
use search::{SearchDb, search_users};

use tokio::sync::mpsc;
use transport::connection::Connection;
use transport::server::{Request, Response};

type ResponseSender = mpsc::UnboundedSender<Response>;

const AUTH_SEARCH_TOKEN_PREFIX: &str = "__auth:";
const PERSONALIZATION_MODEL_ID: &str = "nostr-sage-v1";

enum DispatcherCommand {
    Subscribe { request_id: u32, tx: ResponseSender },
    Unsubscribe { request_id: u32 },
}

#[derive(Debug)]
pub struct DataflowDatabase {
    write_tx: mpsc::UnboundedSender<Request>,
    dispatcher_tx: mpsc::UnboundedSender<DispatcherCommand>,
    request_id: AtomicU32,
    search_db: Arc<Mutex<SearchDb>>,
}

impl DataflowDatabase {
    pub fn new(client: Connection, search_db_root: PathBuf) -> Result<Self, anyhow::Error> {
        let (write_tx, write_rx) = mpsc::unbounded_channel::<Request>();
        let (dispatcher_tx, dispatcher_rx) = mpsc::unbounded_channel::<DispatcherCommand>();

        tokio::spawn(Self::connection_task(client, write_rx, dispatcher_rx));
        let search_db = SearchDb::open_readonly(&search_db_root)?;

        Ok(Self {
            write_tx,
            dispatcher_tx,
            request_id: AtomicU32::new(1),
            search_db: Arc::new(Mutex::new(search_db)),
        })
    }

    async fn connection_task(
        mut client: Connection,
        mut write_rx: mpsc::UnboundedReceiver<Request>,
        mut dispatcher_rx: mpsc::UnboundedReceiver<DispatcherCommand>,
    ) {
        let mut subscribers: HashMap<u32, ResponseSender> = HashMap::new();

        loop {
            tokio::select! {
                Some(request) = write_rx.recv() => {
                    if let Err(err) = client.write(&request) {
                        tracing::warn!("noiad: write request failed: {err:#}");
                    }
                }
                Some(cmd) = dispatcher_rx.recv() => {
                    match cmd {
                        DispatcherCommand::Subscribe { request_id, tx } => {
                            subscribers.insert(request_id, tx);
                        }
                        DispatcherCommand::Unsubscribe { request_id } => {
                            subscribers.remove(&request_id);
                        }
                    }
                }
                result = client.read::<Response>() => {
                    match result {
                        Ok(response) => {
                            let id = response.request_id();

                            if let Some(tx) = subscribers.get(&id) && tx.send(response).is_err() {
                                subscribers.remove(&id);
                            }
                        }
                        Err(err) => {
                            tracing::warn!("noiad: read response failed: {err:#}");
                            break;
                        }
                    }
                }
            }
        }
    }

    fn new_request_id(&self) -> u32 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    fn search_filter_supported(filter: &Filter) -> bool {
        let Some(kinds) = &filter.kinds else {
            return false;
        };

        if !kinds.contains(&Kind::Metadata) {
            return false;
        }

        filter.ids.is_none()
            && filter.authors.is_none()
            && filter.since.is_none()
            && filter.until.is_none()
            && filter.generic_tags.is_empty()
    }

    fn parse_search_text(search_text: &str) -> (String, Option<String>) {
        let mut lexical_tokens = Vec::new();
        let mut auth_pubkey = None;

        for token in search_text.split_whitespace() {
            if let Some(candidate) = token.strip_prefix(AUTH_SEARCH_TOKEN_PREFIX) {
                if candidate.len() == 64 && candidate.chars().all(|c| c.is_ascii_hexdigit()) {
                    auth_pubkey = Some(candidate.to_owned());
                    continue;
                }
            }

            lexical_tokens.push(token);
        }

        (lexical_tokens.join(" "), auth_pubkey)
    }

    fn query_search(search_db: &Mutex<SearchDb>, filter: &Filter) -> Result<Events, DatabaseError> {
        let Some(search_text) = filter.search.as_deref() else {
            return Ok(Events::new(filter));
        };
        let (lexical_search, auth_pubkey) = Self::parse_search_text(search_text);
        let personalize = auth_pubkey
            .as_deref()
            .map(|pubkey| (PERSONALIZATION_MODEL_ID, pubkey));

        tracing::info!(
            "relay search: starting sqlite query search_text={:?} limit={:?} personalize={:?}",
            lexical_search,
            filter.limit,
            personalize
        );

        let search_db = search_db
            .lock()
            .map_err(|err| Self::backend_err(format!("search db mutex poisoned: {err}")))?;
        tracing::info!(
            "relay search: sqlite path={}",
            search_db.paths().sqlite_path.display()
        );
        let limit = filter.limit.unwrap_or(100);
        let results = match search_users(search_db.connection(), &lexical_search, personalize, limit) {
            Ok(results) => results,
            Err(err) => {
                tracing::error!(
                    "relay search: sqlite query failed for search_text={:?} personalize={:?}: {}",
                    lexical_search,
                    personalize,
                    err
                );
                return Ok(Events::new(filter));
            }
        };

        tracing::info!(
            "relay search: sqlite returned {} ranked rows for search_text={:?}",
            results.len(),
            lexical_search
        );

        let mut events = Events::new(filter);
        for result in results {
            match Event::from_json(result.event_json) {
                Ok(event) => {
                    events.insert(event);
                }
                Err(err) => {
                    tracing::error!(
                        "relay search: failed to decode stored event for pubkey={}: {}",
                        result.pubkey,
                        err
                    );
                }
            }
        }

        tracing::info!("relay search: returning {} ranked events", events.len());
        Ok(events)
    }

    fn backend_err<E>(error: E) -> DatabaseError
    where
        E: std::fmt::Display,
    {
        DatabaseError::backend(std::io::Error::other(error.to_string()))
    }
}

impl NostrDatabase for DataflowDatabase {
    fn backend(&self) -> Backend {
        Backend::Custom("dataflow".to_owned())
    }

    fn save_event<'a>(
        &'a self,
        _event: &'a Event,
    ) -> BoxedFuture<'a, Result<SaveEventStatus, DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::NotSupported) })
    }

    fn check_id<'a>(
        &'a self,
        _event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<nostr_database::DatabaseEventStatus, DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::NotSupported) })
    }

    fn event_by_id<'a>(
        &'a self,
        _event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<Option<Event>, DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::NotSupported) })
    }

    fn count(&self, _filter: Filter) -> BoxedFuture<'_, Result<usize, DatabaseError>> {
        // This will be supported in the future
        Box::pin(async move { Err(DatabaseError::NotSupported) })
    }

    fn query(&self, filter: Filter) -> BoxedFuture<'_, Result<Events, DatabaseError>> {
        if filter.search.is_some() && Self::search_filter_supported(&filter) {
            tracing::info!("relay search: using sqlite path for filter {:?}", filter);
            let filter = filter.clone();
            let search_db = Arc::clone(&self.search_db);
            return Box::pin(async move { Self::query_search(&search_db, &filter) });
        }

        if filter.search.is_some() {
            tracing::info!(
                "relay search: rejecting unsupported search filter {:?}",
                filter
            );
            let filter = filter.clone();
            return Box::pin(async move { Ok(Events::new(&filter)) });
        }

        let request_id = self.new_request_id();
        let write_tx = self.write_tx.clone();
        let dispatcher_tx = self.dispatcher_tx.clone();

        Box::pin(async move {
            let (response_tx, mut response_rx) = mpsc::unbounded_channel::<Response>();

            let command = DispatcherCommand::Subscribe {
                request_id,
                tx: response_tx,
            };
            dispatcher_tx
                .send(command)
                .map_err(DatabaseError::backend)?;

            let request = Request {
                request_id,
                filter: filter.clone(),
            };

            write_tx.send(request).map_err(DatabaseError::backend)?;

            let mut events_collection = Events::default();

            while let Some(response) = response_rx.recv().await {
                match response {
                    Response::Result {
                        request_id: res_request_id,
                        events,
                    } if res_request_id == request_id => {
                        events_collection.extend(events);
                    }
                    Response::Eose {
                        request_id: res_request_id,
                    } if res_request_id == request_id => {
                        break;
                    }
                    _ => {}
                }
            }

            let _ = dispatcher_tx.send(DispatcherCommand::Unsubscribe { request_id });

            Ok(events_collection)
        })
    }

    fn delete(&self, _filter: Filter) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::NotSupported) })
    }

    fn wipe(&self) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::NotSupported) })
    }
}
