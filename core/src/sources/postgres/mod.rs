use timely::dataflow::{Scope, StreamCore};

use crate::{
    config::Config,
    sinks::persist_sink::persist_sink,
    sources::postgres::{replication::replication, snapshot::snapshot},
};

pub mod connection;
pub mod parser;
mod replication;
mod snapshot;
pub mod utils;

pub fn render<G>(scope: &G, config: Config) -> StreamCore<G, Vec<()>>
where
    G: Scope<Timestamp = u64>,
{
    let (snapshot_updates, lsn_stream) = snapshot(scope, config.clone());
    let replication_updates = replication(scope, config.clone(), &lsn_stream);
    let updates = snapshot_updates.concat(&replication_updates);

    persist_sink(scope, config.clone(), &updates, &lsn_stream)
}
