use differential_dataflow::VecCollection;
use timely::dataflow::Scope;

use crate::{
    config::Config,
    event::EventRow,
    operators::probe::Handle,
    sources::postgres::{replication::replication, snapshot::snapshot},
};

pub mod connection;
pub mod parser;
mod replication;
mod snapshot;
pub mod utils;

pub fn render<G>(scope: &G, config: Config, probe: &Handle<u64>) -> VecCollection<G, EventRow>
where
    G: Scope<Timestamp = u64>,
{
    let (snapshot_updates, lsn_stream) = snapshot(scope, config.clone());
    let replication_updates = replication(scope, config.clone(), &lsn_stream, probe);

    snapshot_updates.concat(&replication_updates)
}
