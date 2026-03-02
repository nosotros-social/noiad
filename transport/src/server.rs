use anyhow::Result;
use nostr_sdk::{Filter, event::Event};
use serde::{Deserialize, Serialize};

use crate::{
    connection::{Connection, ConnectionHandler},
    socket::{TransportAddr, TransportListener, TransportStream},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub request_id: u32,
    pub filter: Filter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Result { request_id: u32, events: Vec<Event> },
    Eose { request_id: u32 },
}

impl Response {
    pub fn request_id(&self) -> u32 {
        match self {
            Response::Result { request_id, .. } => *request_id,
            Response::Eose { request_id } => *request_id,
        }
    }
}

pub async fn run_server<H>(addr: TransportAddr, handler: impl Fn() -> H) -> Result<()>
where
    H: ConnectionHandler + 'static,
{
    tracing::info!("Server listening on {:?}", addr);
    let listener = TransportListener::bind(&addr).await?;
    loop {
        tracing::info!("Waiting for connection...");
        let stream = listener.accept().await?;
        let handler = handler();

        tracing::info!("Accepted connection from {:?}", stream);
        tokio::spawn(async move {
            let _ = handle_connection(stream, handler).await;
        });
    }
}

async fn handle_connection<H>(stream: TransportStream, mut handler: H) -> Result<()>
where
    H: ConnectionHandler + 'static,
{
    let mut connection = Connection::new(stream);
    loop {
        tokio::select! {
            biased;

            response = connection.read() => {
                let data = response?;
                let _ = handler.send(&data).await;
            },
            updates = handler.recv() => {
                match updates {
                    Ok(data) => {
                        connection.write(&data)?;
                    }
                    Err(e) => {
                        tracing::info!("Error receiving data: {:?}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
