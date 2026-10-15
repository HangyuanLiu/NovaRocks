// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Transparent private REST forwarding with a bounded, credential-free audit.
use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::StatusCode,
    response::Response,
};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct RecordedRestRequest {
    pub method: String,
    pub path_and_query: String,
}
#[derive(Default)]
struct Audit {
    requests: Vec<RecordedRestRequest>,
    overflow: bool,
}
struct ProxyState {
    upstream: String,
    client: reqwest::Client,
    audit: Mutex<Audit>,
}
pub struct RecordingRestCatalog {
    uri: String,
    state: Arc<ProxyState>,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<Result<()>>>,
}
impl RecordingRestCatalog {
    pub fn start(upstream: &str) -> Result<Self> {
        ensure!(
            upstream.starts_with("http://127.0.0.1:") || upstream.starts_with("http://localhost:"),
            "recording REST proxy requires a private loopback upstream"
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let uri = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(ProxyState {
            upstream: upstream.trim_end_matches('/').into(),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(30))
                .build()?,
            audit: Mutex::new(Audit::default()),
        });
        let (stop, closed) = oneshot::channel();
        let serving = state.clone();
        let thread = std::thread::Builder::new()
            .name("recording-rest-catalog".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()?;
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)?;
                    axum::serve(
                        listener,
                        Router::new().fallback(forward).with_state(serving),
                    )
                    .with_graceful_shutdown(async {
                        let _ = closed.await;
                    })
                    .await
                    .context("serve recording REST proxy")
                })
            })?;
        Ok(Self {
            uri,
            state,
            stop: Some(stop),
            thread: Some(thread),
        })
    }
    pub fn uri(&self) -> &str {
        &self.uri
    }
    pub fn requests(&self) -> Result<Vec<RecordedRestRequest>> {
        let audit = self
            .state
            .audit
            .lock()
            .map_err(|_| anyhow::anyhow!("REST audit lock poisoned"))?;
        ensure!(
            !audit.overflow,
            "REST request audit exceeded its declared bound"
        );
        Ok(audit.requests.clone())
    }
    pub fn shutdown(&mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| anyhow::anyhow!("recording REST proxy panicked"))??;
        }
        Ok(())
    }
}
impl Drop for RecordingRestCatalog {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
async fn forward(
    State(state): State<Arc<ProxyState>>,
    request: Request,
) -> Result<Response, StatusCode> {
    let (parts, body) = request.into_parts();
    let path = parts
        .uri
        .path_and_query()
        .map(|v| v.as_str())
        .unwrap_or("/");
    {
        let mut audit = state
            .audit
            .lock()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if audit.requests.len() < 32768 && path.len() <= 4096 {
            audit.requests.push(RecordedRestRequest {
                method: parts.method.to_string(),
                path_and_query: path.into(),
            });
        } else {
            audit.overflow = true;
        }
    }
    let bytes = to_bytes(body, 16 * 1024 * 1024)
        .await
        .map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)?;
    let mut outgoing = state
        .client
        .request(parts.method, format!("{}{path}", state.upstream));
    for (name, value) in &parts.headers {
        if !matches!(
            name.as_str(),
            "host" | "connection" | "transfer-encoding" | "content-length"
        ) {
            outgoing = outgoing.header(name, value);
        }
    }
    let upstream = outgoing
        .body(bytes)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let body = upstream
        .bytes()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let mut response = Response::builder().status(status);
    for (name, value) in &headers {
        if !matches!(
            name.as_str(),
            "connection" | "transfer-encoding" | "content-length"
        ) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from(body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rest_recording_retains_query_and_forwards_original_method_body_and_status() -> Result<()> {
        use std::io::{Read, Write};
        let upstream = std::net::TcpListener::bind("127.0.0.1:0")?;
        upstream.set_nonblocking(true)?;
        let uri = format!("http://{}", upstream.local_addr()?);
        let worker = std::thread::spawn(move || -> Result<()> {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            let mut stream = loop {
                match upstream.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        ensure!(
                            std::time::Instant::now() < deadline,
                            "upstream never accepted a proxy request"
                        );
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(e) => return Err(e.into()),
                }
            };
            stream.set_nonblocking(false)?;
            stream.set_read_timeout(Some(Duration::from_secs(3)))?;
            stream.set_write_timeout(Some(Duration::from_secs(3)))?;
            let mut request = Vec::new();
            let mut byte = [0];
            while !request.ends_with(b"\r\n\r\n") {
                ensure!(request.len() < 8192, "upstream headers exceed test bound");
                stream.read_exact(&mut byte)?;
                request.push(byte[0]);
            }
            ensure!(
                String::from_utf8(request)?
                    .starts_with("POST /v1/namespaces/ns/tables/mv?snapshots=refs HTTP/1.1"),
                "proxy changed original method or query"
            );
            let mut body = [0; 2];
            stream.read_exact(&mut body)?;
            ensure!(&body == b"{}", "proxy changed request body");
            stream.write_all(
                b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
            )?;
            Ok(())
        });
        let mut proxy = None;
        let primary = (|| -> Result<_> {
            proxy = Some(RecordingRestCatalog::start(&uri)?);
            let current = proxy.as_ref().context("proxy missing")?;
            let response = reqwest::blocking::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .build()?
                .post(format!(
                    "{}/v1/namespaces/ns/tables/mv?snapshots=refs",
                    current.uri()
                ))
                .body("{}")
                .send()?;
            Ok((response.status(), response.text()?, current.requests()?))
        })();
        let cleanup = proxy
            .as_mut()
            .map(RecordingRestCatalog::shutdown)
            .transpose();
        let joined = worker
            .join()
            .map_err(|_| anyhow::anyhow!("recording upstream actor panicked"));
        // Join the bounded original actor on failure as well as success.
        cleanup?;
        joined??;
        let (status, body, audit) = primary?;
        ensure!(
            status.as_u16() == 201 && body == "{}",
            "proxy changed upstream response"
        );
        assert_eq!(
            audit,
            vec![RecordedRestRequest {
                method: "POST".into(),
                path_and_query: "/v1/namespaces/ns/tables/mv?snapshots=refs".into()
            }]
        );
        Ok(())
    }
}
