use anyhow::Result;
use std::env;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use transport::connection::Connection;

use nostr_relay_builder::{
    LocalRelay, RelayBuilder,
    builder::{RateLimit, RelayBuilderNip42, RelayBuilderNip42Mode},
};

pub mod database;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_line_number(true).init();

    let core_addr = env::var("CORE_ADDR").unwrap_or_else(|_| "127.0.0.1:5551".into());
    let client = Connection::connect(&core_addr).await?;
    let db_path = env::var("DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("core/data/db"));

    let port: u16 = env::var("PORT")
        .or_else(|_| env::var("port"))
        .unwrap_or_else(|_| "8889".into())
        .parse()
        .expect("Invalid `port` value");

    let database = database::DataflowDatabase::new(client, db_path)?;

    let rate_limit = RateLimit {
        max_reqs: usize::MAX,
        notes_per_minute: u32::MAX,
    };
    let builder = RelayBuilder::default()
        .addr(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        .port(port)
        .database(database)
        .max_connections(1000)
        .nip42(RelayBuilderNip42 {
            mode: RelayBuilderNip42Mode::Read,
        })
        .rate_limit(rate_limit);
    let relay = LocalRelay::run(builder).await?;

    tracing::info!("Relay running at {}", relay.url());

    tokio::signal::ctrl_c().await?;
    Ok(())
}
