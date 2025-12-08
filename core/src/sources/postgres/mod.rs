use differential_dataflow::VecCollection;
use timely::dataflow::Scope;

use crate::{config::Config, event::EventRow, sources::postgres::snapshot::snapshot};

pub mod connection;
pub mod parser;
mod snapshot;
pub mod utils;

pub fn render<G>(scope: &G, config: Config) -> VecCollection<G, EventRow>
where
    G: Scope<Timestamp = u64>,
{
    snapshot(scope, config)
    // TODO: replication
}
