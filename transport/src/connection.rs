use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
};

use crate::{
    server::{Request, Response},
    socket::{TransportAddr, TransportReadHalf, TransportStream},
};

#[async_trait]
pub trait ConnectionHandler: fmt::Debug + Send {
    async fn send(&mut self, data: &Request) -> Result<()>;
    async fn recv(&mut self) -> Result<Response>;
}

#[derive(Debug)]
pub struct Connection {
    pub tx: mpsc::UnboundedSender<Vec<u8>>,
    pub rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl Connection {
    pub fn new(stream: TransportStream) -> Self {
        let (reader, writer) = stream.into_split();

        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (out_tx, out_rx) = mpsc::unbounded_channel();

        tokio::spawn(read_task(reader, in_tx));
        tokio::spawn(write_task(writer, out_rx));

        Connection {
            tx: out_tx,
            rx: in_rx,
        }
    }

    pub async fn connect(addr: &str) -> Result<Self> {
        let addr = TransportAddr::from_str(addr)?;
        let stream = TransportStream::connect(&addr)
            .await
            .map_err(|e| anyhow!("failed to connect to {:?}: {}", addr, e))?;
        Ok(Connection::new(stream))
    }

    pub fn write<T>(&self, msg: &T) -> Result<()>
    where
        T: Serialize,
    {
        let bytes = serde_json::to_vec(msg)?;
        self.tx.send(bytes)?;
        Ok(())
    }

    pub async fn read<T>(&mut self) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let bytes = self
            .rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("connection closed"))?;
        Ok(serde_json::from_slice::<T>(&bytes)?)
    }
}

async fn read_task(mut reader: TransportReadHalf, tx: mpsc::UnboundedSender<Vec<u8>>) {
    loop {
        let len = match reader.read_u64_le().await {
            Ok(len) => len,
            Err(_) => break,
        };

        let mut buf = vec![0u8; len as usize];
        if reader.read_exact(&mut buf).await.is_err() {
            break;
        }

        if tx.send(buf).is_err() {
            break;
        }
    }
}

async fn write_task(
    mut writer: crate::socket::TransportWriteHalf,
    mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    while let Some(bytes) = rx.recv().await {
        if writer.write_u64_le(bytes.len() as u64).await.is_err() {
            break;
        }
        if writer.write_all(&bytes).await.is_err() {
            break;
        }
    }
}
