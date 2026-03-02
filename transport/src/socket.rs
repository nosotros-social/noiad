use anyhow::Result;
use std::{
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{
        TcpListener, TcpStream, UnixListener, UnixStream,
        tcp::{OwnedReadHalf as TcpReadHalf, OwnedWriteHalf as TcpWriteHalf},
        unix::{OwnedReadHalf as UnixReadHalf, OwnedWriteHalf as UnixWriteHalf},
    },
};

#[derive(Debug)]
pub enum TransportStream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl TransportStream {
    pub fn into_split(self) -> (TransportReadHalf, TransportWriteHalf) {
        match self {
            TransportStream::Tcp(s) => {
                let (r, w) = s.into_split();
                (TransportReadHalf::Tcp(r), TransportWriteHalf::Tcp(w))
            }
            TransportStream::Unix(s) => {
                let (r, w) = s.into_split();
                (TransportReadHalf::Unix(r), TransportWriteHalf::Unix(w))
            }
        }
    }

    pub async fn connect(addr: &TransportAddr) -> Result<Self> {
        match addr {
            TransportAddr::Tcp(socket_addr) => {
                let stream = TcpStream::connect(socket_addr).await?;
                Ok(TransportStream::Tcp(stream))
            }
            TransportAddr::Unix(path) => {
                let stream = UnixStream::connect(path).await?;
                Ok(TransportStream::Unix(stream))
            }
        }
    }
}

pub enum TransportListener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

impl TransportListener {
    pub async fn bind(addr: &TransportAddr) -> Result<Self> {
        match addr {
            TransportAddr::Tcp(socket_addr) => {
                let listener = TcpListener::bind(socket_addr).await?;
                Ok(TransportListener::Tcp(listener))
            }
            TransportAddr::Unix(path) => {
                let listener = UnixListener::bind(path)?;
                Ok(TransportListener::Unix(listener))
            }
        }
    }

    pub async fn accept(&self) -> Result<TransportStream> {
        match self {
            TransportListener::Tcp(tcp_listener) => {
                let (socket, _) = tcp_listener.accept().await?;
                Ok(TransportStream::Tcp(socket))
            }
            TransportListener::Unix(unix_listener) => {
                let (socket, _) = unix_listener.accept().await?;
                Ok(TransportStream::Unix(socket))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportAddr {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

impl FromStr for TransportAddr {
    type Err = anyhow::Error;

    fn from_str(addr: &str) -> Result<Self> {
        if addr.split_once(':').is_some()
            && let Ok(socket) = addr.parse::<SocketAddr>()
        {
            Ok(TransportAddr::Tcp(socket))
        } else {
            Ok(TransportAddr::Unix(PathBuf::from(addr)))
        }
    }
}

pub enum TransportReadHalf {
    Tcp(TcpReadHalf),
    Unix(UnixReadHalf),
}

impl AsyncRead for TransportReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TransportReadHalf::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            TransportReadHalf::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

pub enum TransportWriteHalf {
    Tcp(TcpWriteHalf),
    Unix(UnixWriteHalf),
}

impl AsyncWrite for TransportWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            TransportWriteHalf::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            TransportWriteHalf::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TransportWriteHalf::Tcp(s) => Pin::new(s).poll_flush(cx),
            TransportWriteHalf::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TransportWriteHalf::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            TransportWriteHalf::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::socket::TransportAddr;

    #[tokio::test]
    async fn assert_transport_addr() {
        assert_eq!(
            TransportAddr::from_str("127.0.0.1:8000").unwrap(),
            TransportAddr::Tcp(std::net::SocketAddr::from(([127, 0, 0, 1], 8000)))
        );
        assert_eq!(
            TransportAddr::from_str("/var/run/socket.sock").unwrap(),
            TransportAddr::Unix(std::path::PathBuf::from("/var/run/socket.sock"))
        );
    }
}
