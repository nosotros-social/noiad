use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use nostr_database::nostr::event::{Event, EventId};
use nostr_database::nostr::filter::Filter;
use nostr_database::nostr::util::BoxedFuture;
use nostr_database::{Backend, DatabaseError, Events, NostrDatabase, SaveEventStatus};

use tokio::sync::mpsc;
use transport::connection::Connection;
use transport::server::{Request, Response};

type ResponseSender = mpsc::UnboundedSender<Response>;

enum DispatcherCommand {
    Subscribe { request_id: u32, tx: ResponseSender },
    Unsubscribe { request_id: u32 },
}

#[derive(Debug)]
pub struct DataflowDatabase {
    write_tx: mpsc::UnboundedSender<Request>,
    dispatcher_tx: mpsc::UnboundedSender<DispatcherCommand>,
    request_id: AtomicU32,
}

impl DataflowDatabase {
    pub fn new(client: Connection) -> Self {
        let (write_tx, write_rx) = mpsc::unbounded_channel::<Request>();
        let (dispatcher_tx, dispatcher_rx) = mpsc::unbounded_channel::<DispatcherCommand>();

        tokio::spawn(Self::connection_task(client, write_rx, dispatcher_rx));

        Self {
            write_tx,
            dispatcher_tx,
            request_id: AtomicU32::new(1),
        }
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
