use anyhow::Result;
use nostrpg::NostrPg;
use std::env;

use nostr_relay_builder::{LocalRelay, RelayBuilder, builder::RateLimit};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_line_number(true).init();

    let port: u16 = env::var("port")
        .unwrap_or("8889".into())
        .parse()
        .expect("Invalid `port` value");

    let database_url = env::var("DATABASE_URL")
        .unwrap_or("postgres://postgres:postgres@localhost:5432/nostr".into());

    let database = NostrPg::new(database_url, None).await?;

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
