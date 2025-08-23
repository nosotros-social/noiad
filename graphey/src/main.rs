use anyhow::Result;
use std::env;

use nostr_relay_builder::{LocalRelay, RelayBuilder, builder::RateLimit};
use nostr_surrealdb::NostrSurrealDB;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_line_number(true).init();

    let port: u16 = env::var("port")
        .unwrap_or("8889".into())
        .parse()
        .expect("Invalid `port` value");

    let endpoint = env::var("endpoint").unwrap_or("http://127.0.0.1:8000".into());

    let surreal = NostrSurrealDB::new().open(&endpoint).await?;

    let rate_limit = RateLimit {
        max_reqs: usize::MAX,
        notes_per_minute: u32::MAX,
    };
    let builder = RelayBuilder::default()
        .port(port)
        .database(surreal)
        .rate_limit(rate_limit);
    let relay = LocalRelay::run(builder).await?;

    tracing::info!("Relay running at {}", relay.url());

    tokio::signal::ctrl_c().await?;
    Ok(())
}
