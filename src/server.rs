//! Canalis's own accept loop, instead of `axum::serve`, for the
//! connection settings that doesn't expose:
//!
//! - a header-read timeout (CANALIS_HEADER_READ_TIMEOUT_SECONDS, default
//!   10): a client that opens a connection and never finishes sending its
//!   request headers is dropped instead of holding a connection forever
//!   (slowloris);
//! - 10,000 concurrent HTTP/2 streams per connection rather than 200, for
//!   the waiting-room streams other Canalis nodes forward here.
//!
//! Everything else is what `axum::serve` does: HTTP/1 and HTTP/2 on one
//! port, WebSocket upgrades, the client's address as `ConnectInfo`, and a
//! graceful shutdown that lets open connections finish.

use axum::Router;
use axum::extract::ConnectInfo;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use std::time::Duration;
use tower::ServiceExt;

pub struct ServerConfig {
    pub header_read_timeout: Duration,
    pub max_concurrent_streams: u32,
}

impl ServerConfig {
    pub fn from_env() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(crate::env_u64("CANALIS_HEADER_READ_TIMEOUT_SECONDS", 10).max(1)),
            max_concurrent_streams: 10_000,
        }
    }
}

/// Serves `router` until `shutdown` completes, then waits for open
/// connections to finish (the caller bounds how long).
pub async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let mut builder = Builder::new(TokioExecutor::new());
    builder.http1().timer(TokioTimer::new()).header_read_timeout(config.header_read_timeout);
    builder.http2().timer(TokioTimer::new()).max_concurrent_streams(config.max_concurrent_streams);
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);

    loop {
        let (stream, remote) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    // Out of open files, most likely: wait a moment rather
                    // than spin.
                    tracing::warn!("accept failed: {err}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = &mut shutdown => break,
        };
        let _ = stream.set_nodelay(true);
        let router = router.clone();
        let service = hyper::service::service_fn(move |mut request: hyper::Request<hyper::body::Incoming>| {
            request.extensions_mut().insert(ConnectInfo(remote));
            router.clone().oneshot(request.map(axum::body::Body::new))
        });
        let connection = builder.serve_connection_with_upgrades(TokioIo::new(stream), service).into_owned();
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::debug!("connection from {remote} ended: {err}");
            }
        });
    }
    graceful.shutdown().await;
}

/// Serves on an ephemeral local port for tests; returns the address.
#[cfg(test)]
pub async fn serve_for_test(router: Router, config: ServerConfig) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, router, config, std::future::pending()));
    addr
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn a_client_that_never_finishes_its_headers_is_dropped() {
        let router = Router::new().route("/health", axum::routing::get(|| async { "ok" }));
        let addr = serve_for_test(router, ServerConfig { header_read_timeout: Duration::from_millis(300), max_concurrent_streams: 100 }).await;

        let mut slow = tokio::net::TcpStream::connect(addr).await.unwrap();
        slow.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n").await.unwrap(); // never finished
        let mut buf = [0u8; 256];
        let read = tokio::time::timeout(Duration::from_secs(3), slow.read(&mut buf)).await;
        assert!(read.is_ok(), "the server should have given up on it well within 3s");

        // A normal request still works.
        let ok = reqwest::get(format!("http://{addr}/health")).await.unwrap().text().await.unwrap();
        assert_eq!(ok, "ok");
    }

    #[tokio::test]
    async fn the_client_address_reaches_handlers() {
        async fn whoami(ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>) -> String {
            addr.ip().to_string()
        }
        let router = Router::new().route("/me", axum::routing::get(whoami));
        let addr = serve_for_test(router, ServerConfig::from_env()).await;
        assert_eq!(reqwest::get(format!("http://{addr}/me")).await.unwrap().text().await.unwrap(), "127.0.0.1");
    }
}
