use core::event::Event;
use timely::dataflow::Scope;

use differential_dataflow::Collection;

use crate::{config::Config, sources::postgres::snapshot::snapshot};

pub mod connection;
pub mod parser;
mod snapshot;
pub mod utils;

pub fn render<G>(scope: G, config: Config) -> Collection<G, Event, isize>
where
    G: Scope<Timestamp = u64>,
{
    snapshot(scope, config)
    // TODO: replication
}
