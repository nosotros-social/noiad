use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use nostr_sdk::filter::MatchEventOptions;
use timely::{communication::Allocate, worker::Worker as TimelyWorker};
use transport::server::{Request, Response};

use crate::{
    query::filter::{DataflowFilter, effective_query_limit},
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
        let mut candidate_filter = request.filter.clone();
        let response_limit = effective_query_limit(candidate_filter.limit);
        candidate_filter.limit = Some(response_limit);

        if response_limit == 0 {
            let _ = self.res_tx.send(Response::Result {
                request_id,
                events: vec![],
            });
            let _ = self.res_tx.send(Response::Eose { request_id });
            return;
        }

        let Some(filter) = DataflowFilter::from_nostr_filter(candidate_filter, &self.state.persist)
        else {
            tracing::warn!(
                "[Worker {}] request_id={} filter conversion returned none: {:?}",
                self.worker_id,
                request_id,
                request.filter
            );
            let _ = self.res_tx.send(Response::Result {
                request_id,
                events: vec![],
            });
            let _ = self.res_tx.send(Response::Eose { request_id });
            return;
        };

        match self.state.query(&filter) {
            Ok(event_ids) => {
                let mut events = Vec::new();

                for id in event_ids {
                    if events.len() >= response_limit {
                        break;
                    }

                    match self.state.persist.get_original_event(id) {
                        Ok(Some(event))
                            if request.filter.match_event(&event, MatchEventOptions::new()) =>
                        {
                            events.push(event);
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(
                                "[Worker {}] request_id={} failed to resolve event_id={}: {:?}",
                                self.worker_id,
                                request_id,
                                id,
                                err
                            );
                        }
                    }
                }
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
