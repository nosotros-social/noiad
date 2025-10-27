use std::{env, ops::Deref, str::FromStr};

use anyhow::Result;
use tokio_postgres::{Client, NoTls};

#[derive(Debug, Clone)]
pub struct PostgresConnection(pub tokio_postgres::Config);

impl PostgresConnection {
    pub fn from_env() -> Self {
        let connection_str = env::var("DB_URL").expect("DB_URL must be set");
        let config = tokio_postgres::Config::from_str(&connection_str).unwrap();
        Self(config)
    }

    pub async fn connect(self) -> Result<Client> {
        let config = self.0;
        connect_internal(&config).await
    }

    /// Connect to Postgres in replication mode
    pub async fn connect_replication(self) -> Result<Client> {
        let config = self.0;
        connect_internal(
            config
                .clone()
                .replication_mode(tokio_postgres::config::ReplicationMode::Logical)
                .deref(),
        )
        .await
    }
}

async fn connect_internal(config: &tokio_postgres::Config) -> Result<Client> {
    // This is to work with multiple timely workers each having their own tokio runtime
    let runtime =
        tokio::runtime::Handle::try_current().expect("No tokio runtime on this worker thread");
    let (client, connection) = config.connect(NoTls).await.unwrap();
    runtime.spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!("pg conn err: {e}");
        }
    });
    Ok(client)
}
