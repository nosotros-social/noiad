use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use timely::{communication::Allocate, worker::Worker as TimelyWorker};
use transport::server::{Request, Response};

use crate::{
    query::filter::{DataflowFilter, DataflowFilterTags},
    state::State,
};

pub struct Worker<'a, A: Allocate> {
    pub worker_id: usize,
    pub timely_worker: &'a mut TimelyWorker<A>,
    pub state: State,
    pub req_rx: Receiver<Request>,
    pub res_tx: Sender<Response>,
}

impl<'a, A: Allocate> Worker<'a, A> {
    pub fn run(&mut self) {
        tracing::info!("[Worker {}] running...", self.worker_id);

        loop {
            loop {
                match self.req_rx.try_recv() {
                    Ok(request) => self.handle_request(request),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        tracing::error!("[Worker {}] req_rx disconnected", self.worker_id);
                        return;
                    }
                }
            }

            let _ = self
                .timely_worker
                .step_or_park(Some(Duration::from_millis(2)));
        }
    }

    fn handle_request(&mut self, request: Request) {
        let request_id = request.request_id;
        let Some(filter) =
            DataflowFilter::from_nostr_filter(request.filter.clone(), &self.state.persist)
        else {
            tracing::error!(
                "[Worker {}] DataflowFilter: None (no matching interned values)",
                self.worker_id
            );
            let _ = self.res_tx.send(Response::Result {
                request_id,
                events: vec![],
            });
            let _ = self.res_tx.send(Response::Eose { request_id });
            return;
        };

        match self.state.query(&filter) {
            Ok(event_rows) => {
                let event_ids: Vec<u32> = event_rows.iter().map(|row| row.id).collect();
                let event_ids_len = event_ids.len();
                let events = event_ids
                    .into_iter()
                    .filter_map(|id| self.state.persist.get_original_event(id).ok())
                    .flatten()
                    .collect::<Vec<nostr_sdk::event::Event>>();

                tracing::debug!(
                    "[Worker {}] [request_id={}] {} rows retrieved ({} row resolved)",
                    self.worker_id,
                    request_id,
                    event_ids_len,
                    events.len(),
                );
                let _ = self.res_tx.send(Response::Result { request_id, events });
            }
            Err(err) => {
                tracing::error!(
                    "[Worker {}] query error for request {}: {:?}",
                    self.worker_id,
                    request_id,
                    err
                );
                let _ = self.res_tx.send(Response::Result {
                    request_id,
                    events: vec![],
                });
            }
        }

        let _ = self.res_tx.send(Response::Eose { request_id });
    }
}
