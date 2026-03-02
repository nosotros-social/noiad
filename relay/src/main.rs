use anyhow::Result;
use std::env;
use transport::connection::Connection;

use nostr_relay_builder::{LocalRelay, RelayBuilder, builder::RateLimit};

pub mod database;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_line_number(true).init();

    let client = Connection::connect("127.0.0.1:5551").await?;

    let port: u16 = env::var("port")
        .unwrap_or("8889".into())
        .parse()
        .expect("Invalid `port` value");

    let database = database::DataflowDatabase::new(client);

    let rate_limit = RateLimit {
        max_reqs: usize::MAX,
        notes_per_minute: u32::MAX,
    };
    let builder = RelayBuilder::default()
        .port(port)
        .database(database)
        .max_connections(1000)
        .rate_limit(rate_limit);
    let relay = LocalRelay::run(builder).await?;

    tracing::info!("Relay running at {}", relay.url());

    tokio::signal::ctrl_c().await?;
    Ok(())
}
