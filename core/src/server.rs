use anyhow::Result;
use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use transport::{
    connection::ConnectionHandler,
    server::{Request, Response},
};

#[derive(Debug)]
pub struct Handler {
    pub req_txs: Arc<Vec<Sender<Request>>>,
    pub resp_rx: mpsc::UnboundedReceiver<Response>,
    recv_task: tokio::task::JoinHandle<()>,
}

type ReqId = u32;
type Results = Vec<nostr_sdk::Event>;
type Eose = usize;
type PendingRequests = HashMap<ReqId, (Results, Eose)>;

impl Handler {
    pub fn new(req_txs: Arc<Vec<Sender<Request>>>, res_rxs: Arc<Vec<Receiver<Response>>>) -> Self {
        let (resp_tx, resp_rx) = mpsc::unbounded_channel();
        let worker_count = res_rxs.len();

        let recv_task = tokio::task::spawn_blocking(move || {
            let mut pending: PendingRequests = HashMap::new();

            loop {
                let mut sel = crossbeam_channel::Select::new();
                for rx in res_rxs.iter() {
                    sel.recv(rx);
                }

                let oper = sel.select();
                let idx = oper.index();
                let response = match oper.recv(&res_rxs[idx]) {
                    Ok(r) => r,
                    Err(_) => break,
                };

                match response {
                    Response::Result { request_id, events } => {
                        let (results, _) =
                            pending.entry(request_id).or_insert_with(|| (Vec::new(), 0));
                        results.extend(events);
                    }
                    Response::Eose { request_id } => {
                        let (_, eoses) =
                            pending.entry(request_id).or_insert_with(|| (Vec::new(), 0));

                        *eoses += 1;

                        if *eoses >= worker_count {
                            let (events, _) = pending.remove(&request_id).unwrap();

                            if resp_tx
                                .send(Response::Result { request_id, events })
                                .is_err()
                            {
                                break;
                            }
                            if resp_tx.send(Response::Eose { request_id }).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self {
            req_txs,
            resp_rx,
            recv_task,
        }
    }

    pub fn build_worker_channels(
        worker_count: usize,
    ) -> (
        Arc<Vec<Sender<Request>>>,
        Arc<Vec<Receiver<Request>>>,
        Arc<Vec<Sender<Response>>>,
        Arc<Vec<Receiver<Response>>>,
    ) {
        let (req_txs, req_rxs): (Vec<_>, Vec<_>) = (0..worker_count)
            .map(|_| crossbeam_channel::unbounded::<Request>())
            .unzip();
        let (res_txs, res_rxs): (Vec<_>, Vec<_>) = (0..worker_count)
            .map(|_| crossbeam_channel::unbounded::<Response>())
            .unzip();

        (
            Arc::new(req_txs),
            Arc::new(req_rxs),
            Arc::new(res_txs),
            Arc::new(res_rxs),
        )
    }

    pub fn shutdown(self) {
        drop(self.req_txs);
        self.recv_task.abort();
    }
}

#[async_trait]
impl ConnectionHandler for Handler {
    async fn send(&mut self, request: &Request) -> Result<()> {
        // Broadcast request to all workers
        for tx in self.req_txs.iter() {
            tx.send(request.clone())?;
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Response> {
        self.resp_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("channel closed"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_sdk::{EventBuilder, Keys};
    use transport::connection::ConnectionHandler;

    fn make_test_event(keys: Keys) -> nostr_sdk::Event {
        EventBuilder::text_note("Hello")
            .sign_with_keys(&keys)
            .unwrap()
    }

    struct MockWorker {
        res_tx: Sender<Response>,
        req_rx: Receiver<Request>,
    }

    impl MockWorker {
        fn send_results(&self, request_id: u32, events: Vec<nostr_sdk::Event>) {
            self.res_tx
                .send(Response::Result { request_id, events })
                .unwrap();
        }

        fn send_eose(&self, request_id: u32) {
            self.res_tx.send(Response::Eose { request_id }).unwrap();
        }

        fn recv_request(&self) -> Request {
            self.req_rx
                .try_recv()
                .expect("worker should have received a request")
        }
    }

    fn make_handler(workers: usize) -> (Handler, Vec<MockWorker>) {
        let (req_txs, req_rxs, res_txs, res_rxs) = Handler::build_worker_channels(workers);

        let handler = Handler::new(req_txs, res_rxs);

        let workers = (0..workers)
            .map(|i| MockWorker {
                res_tx: res_txs[i].clone(),
                req_rx: req_rxs[i].clone(),
            })
            .collect::<Vec<_>>();

        (handler, workers)
    }

    async fn assert_recv_request(
        handler: &mut Handler,
        expected_req_id: u32,
        expected_events: Vec<nostr_sdk::Event>,
    ) {
        match handler.recv().await.unwrap() {
            Response::Result { request_id, events } => {
                assert_eq!(request_id, expected_req_id);
                assert_eq!(events.len(), expected_events.len());
                expected_events
                    .iter()
                    .for_each(|e| assert!(events.contains(e)));
            }
            other => panic!("expected Result, got {other:?}"),
        }
        match handler.recv().await.unwrap() {
            Response::Eose { request_id } => assert_eq!(request_id, expected_req_id),
            other => panic!("expected Eose, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn assert_server_workers() {
        let (mut handler, workers) = make_handler(3);

        let key1 = Keys::generate();
        let key2 = Keys::generate();
        let pubkey1 = key1.public_key();
        let pubkey2 = key2.public_key();

        let event1 = make_test_event(key1.clone());
        let event2 = make_test_event(key1.clone());
        let event3 = make_test_event(key2.clone());
        let event4 = make_test_event(key2.clone());

        let req_id_1 = 1;
        handler
            .send(&Request {
                request_id: req_id_1,
                filter: nostr_sdk::Filter::new()
                    .kind(nostr_sdk::Kind::TextNote)
                    .author(pubkey1),
            })
            .await
            .unwrap();
        for w in workers.iter() {
            assert_eq!(w.recv_request().request_id, req_id_1);
        }

        let req_id_2 = 2;
        handler
            .send(&Request {
                request_id: req_id_2,
                filter: nostr_sdk::Filter::new()
                    .kind(nostr_sdk::Kind::TextNote)
                    .author(pubkey2),
            })
            .await
            .unwrap();
        for w in workers.iter() {
            assert_eq!(w.recv_request().request_id, req_id_2);
        }

        workers[0].send_results(req_id_1, vec![event1.clone()]);
        workers[0].send_eose(req_id_1);
        workers[0].send_results(req_id_2, vec![event3.clone()]);
        workers[0].send_eose(req_id_2);
        // Worker 2 will send results for req_id_2 before req_id_1
        workers[1].send_results(req_id_2, vec![event4.clone()]);
        workers[1].send_eose(req_id_2);
        workers[1].send_results(req_id_1, vec![event2.clone()]);
        workers[1].send_eose(req_id_1);

        workers[2].send_eose(req_id_1);
        workers[2].send_eose(req_id_2);

        assert_recv_request(&mut handler, req_id_2, vec![event3.clone(), event3.clone()]).await;
        assert_recv_request(&mut handler, req_id_1, vec![event1.clone(), event2.clone()]).await;
    }
}
