// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::net::{SocketAddr, TcpListener};
use std::thread::JoinHandle;

use axum::{Router, routing::get};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;

/// Frontend-owned HTTP listener for metrics in a dedicated FE process.
///
/// The gRPC report endpoint also serves `/metrics` for compatibility. A
/// role=fe process nevertheless owns the configured HTTP listener so the
/// native FE/BE deployment has one stable metrics endpoint per role.
pub(crate) struct MetricsHttpServer {
    shutdown_tx: Option<watch::Sender<bool>>,
    join_handle: Option<JoinHandle<()>>,
}

impl MetricsHttpServer {
    pub(crate) fn start(host: &str, port: u16) -> Result<Self, String> {
        let bind_addr = parse_metrics_bind_addr(host, port)
            .map_err(|error| format!("parse frontend metrics HTTP bind address failed: {error}"))?;
        let listener = TcpListener::bind(bind_addr).map_err(|error| {
            format!("bind frontend metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("configure frontend metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let join_handle = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("failed to build frontend metrics HTTP runtime: {error}");
                    return;
                }
            };
            runtime.block_on(async move {
                let listener = match TokioTcpListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        eprintln!("failed to create frontend metrics HTTP listener: {error}");
                        return;
                    }
                };
                let app = Router::new().route("/metrics", get(super::handle_metrics));
                if let Err(error) = axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        while !*shutdown_rx.borrow() {
                            if shutdown_rx.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await
                {
                    eprintln!("frontend metrics HTTP server exited with error: {error}");
                }
            });
        });
        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        })
    }

    pub(crate) fn stop(mut self) -> Result<(), String> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "frontend metrics HTTP server thread panicked".to_string())?;
        }
        Ok(())
    }
}

fn parse_metrics_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let bare = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|error| format!("parse frontend metrics bind addr '{formatted}' failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_bind_addr_accepts_ipv4_and_ipv6_literals() {
        assert_eq!(
            parse_metrics_bind_addr("127.0.0.1", 9070).expect("parse IPv4"),
            "127.0.0.1:9070"
                .parse::<SocketAddr>()
                .expect("IPv4 address")
        );
        assert_eq!(
            parse_metrics_bind_addr("::1", 9070).expect("parse bare IPv6"),
            "[::1]:9070".parse::<SocketAddr>().expect("IPv6 address")
        );
    }
}
