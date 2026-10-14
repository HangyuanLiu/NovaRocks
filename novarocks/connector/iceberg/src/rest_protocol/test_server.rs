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

//! A scripted HTTP/1.1 server for REST protocol tests, standard library only.
//!
//! One thread per connection, so concurrent first requests are served
//! concurrently. Every request is answered from a [`Script`] matched by
//! method and path, and recorded (method, target, headers, body). Dropping
//! the server shuts every connection down and joins every thread.
//!
//! Limits: request bodies must carry `Content-Length` (reqwest sends it for
//! every buffered body); a request whose first byte is not an ASCII letter
//! (for example a TLS ClientHello) closes the connection unrecorded.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// One recorded request.
#[derive(Clone, Debug)]
pub(crate) struct RecordedRequest {
    pub(crate) method: String,
    /// The request target as sent: path and query.
    pub(crate) target: String,
    /// Header names lower-cased, in arrival order.
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl RecordedRequest {
    pub(crate) fn path(&self) -> &str {
        self.target
            .split_once('?')
            .map_or(self.target.as_str(), |(path, _)| path)
    }

    pub(crate) fn query(&self) -> Option<&str> {
        self.target.split_once('?').map(|(_, query)| query)
    }

    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(header, _)| *header == name)
            .map(|(_, value)| value.as_str())
    }

    /// Headers as a sorted name -> value map.
    pub(crate) fn header_map(&self) -> std::collections::BTreeMap<String, String> {
        self.headers.iter().cloned().collect()
    }

    /// The body decoded as `application/x-www-form-urlencoded`, sorted.
    pub(crate) fn form(&self) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = url::form_urlencoded::parse(&self.body)
            .into_owned()
            .collect();
        pairs.sort();
        pairs
    }
}

/// How the body of a scripted response is written.
#[derive(Clone, Debug)]
pub(crate) enum BodyPlan {
    /// One write, with `Content-Length`.
    Fixed(Vec<u8>),
    /// `Transfer-Encoding: chunked`; each chunk is written after its delay.
    Chunked(Vec<(Duration, Vec<u8>)>),
    /// `Content-Length: total`, written in `piece`-byte writes until done or
    /// until a write fails; the outcome is recorded (see
    /// [`TestServer::outcomes`]).
    Streamed { total: usize, piece: usize },
}

/// A scripted response.
#[derive(Clone, Debug)]
pub(crate) struct ScriptedResponse {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: BodyPlan,
    /// Waited before the status line is written.
    pub(crate) delay: Duration,
}

/// The answer to one request.
#[derive(Clone, Debug)]
pub(crate) enum Reply {
    Respond(ScriptedResponse),
    /// Writes the status line only, then closes the connection.
    CloseAfterStatusLine(u16),
    /// Reads the request, then closes the connection without answering.
    CloseAfterRequest,
    /// Reads the request and never answers; the connection stays open until
    /// the client closes it or the server shuts down.
    NeverAnswer,
}

impl Reply {
    pub(crate) fn status(status: u16) -> Self {
        Self::bytes(status, Vec::new())
    }

    pub(crate) fn bytes(status: u16, body: Vec<u8>) -> Self {
        Self::Respond(ScriptedResponse {
            status,
            headers: Vec::new(),
            body: BodyPlan::Fixed(body),
            delay: Duration::ZERO,
        })
    }

    pub(crate) fn json(status: u16, body: &serde_json::Value) -> Self {
        Self::bytes(status, body.to_string().into_bytes())
            .with_header("content-type", "application/json")
    }

    /// A `/v1/config` answer; `endpoints: None` omits the field.
    pub(crate) fn config(
        defaults: &[(&str, &str)],
        overrides: &[(&str, &str)],
        endpoints: Option<&[&str]>,
    ) -> Self {
        let map = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_string(), serde_json::Value::from(*value)))
                .collect::<serde_json::Map<_, _>>()
        };
        let mut body = serde_json::json!({
            "defaults": map(defaults),
            "overrides": map(overrides),
        });
        if let Some(endpoints) = endpoints {
            body["endpoints"] = serde_json::Value::from(endpoints.to_vec());
        }
        Self::json(200, &body)
    }

    /// A successful OAuth2 token answer.
    pub(crate) fn token(access_token: &str) -> Self {
        Self::json(
            200,
            &serde_json::json!({
                "access_token": access_token,
                "token_type": "bearer",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            }),
        )
    }

    pub(crate) fn redirect(status: u16, location: &str) -> Self {
        Self::status(status).with_header("location", location)
    }

    /// Panics on a reply that is not [`Reply::Respond`].
    pub(crate) fn with_header(mut self, name: &str, value: &str) -> Self {
        self.response_mut()
            .headers
            .push((name.to_string(), value.to_string()));
        self
    }

    /// Panics on a reply that is not [`Reply::Respond`].
    pub(crate) fn with_delay(mut self, delay: Duration) -> Self {
        self.response_mut().delay = delay;
        self
    }

    /// Panics on a reply that is not [`Reply::Respond`].
    pub(crate) fn with_body(mut self, body: BodyPlan) -> Self {
        self.response_mut().body = body;
        self
    }

    fn response_mut(&mut self) -> &mut ScriptedResponse {
        match self {
            Self::Respond(response) => response,
            other => panic!("not a scripted response: {other:?}"),
        }
    }
}

type Responder = Box<dyn Fn(&RecordedRequest, usize) -> Reply + Send + Sync>;

struct Route {
    method: String,
    path: String,
    responder: Responder,
    hits: AtomicUsize,
}

/// Answers by method and exact path (query ignored); unmatched requests get
/// `404` with an empty body.
#[derive(Default)]
pub(crate) struct Script {
    routes: Vec<Route>,
}

impl Script {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Every matching request gets `reply`.
    pub(crate) fn on(self, method: &str, path: &str, reply: Reply) -> Self {
        self.on_fn(method, path, move |_, _| reply.clone())
    }

    /// The n-th matching request gets `replies[n]`; the last one repeats.
    pub(crate) fn on_sequence(self, method: &str, path: &str, replies: Vec<Reply>) -> Self {
        assert!(!replies.is_empty(), "a sequence needs at least one reply");
        self.on_fn(method, path, move |_, index| {
            replies[index.min(replies.len() - 1)].clone()
        })
    }

    /// `responder` gets the request and its zero-based index among the
    /// requests matching this route.
    pub(crate) fn on_fn(
        mut self,
        method: &str,
        path: &str,
        responder: impl Fn(&RecordedRequest, usize) -> Reply + Send + Sync + 'static,
    ) -> Self {
        self.routes.push(Route {
            method: method.to_ascii_uppercase(),
            path: path.to_string(),
            responder: Box::new(responder),
            hits: AtomicUsize::new(0),
        });
        self
    }

    fn reply(&self, request: &RecordedRequest) -> Reply {
        self.routes
            .iter()
            .find(|route| route.method == request.method && route.path == request.path())
            .map_or_else(
                || Reply::status(404),
                |route| (route.responder)(request, route.hits.fetch_add(1, Ordering::SeqCst)),
            )
    }
}

/// How a [`BodyPlan::Streamed`] body ended.
#[derive(Clone, Debug)]
pub(crate) struct StreamOutcome {
    pub(crate) target: String,
    pub(crate) written: usize,
    pub(crate) total: usize,
    /// Whether every byte was written before the connection failed.
    pub(crate) complete: bool,
}

struct ServerState {
    script: Script,
    requests: Mutex<Vec<RecordedRequest>>,
    outcomes: Mutex<Vec<StreamOutcome>>,
    shutdown: AtomicBool,
    connections: Mutex<Vec<(TcpStream, JoinHandle<()>)>>,
}

/// The running server; dropping it shuts it down.
pub(crate) struct TestServer {
    address: SocketAddr,
    state: Arc<ServerState>,
    acceptor: Option<JoinHandle<()>>,
}

impl TestServer {
    pub(crate) fn start(script: Script) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener
            .set_nonblocking(true)
            .expect("make the test listener non-blocking");
        let address = listener.local_addr().expect("test server address");
        let state = Arc::new(ServerState {
            script,
            requests: Mutex::new(Vec::new()),
            outcomes: Mutex::new(Vec::new()),
            shutdown: AtomicBool::new(false),
            connections: Mutex::new(Vec::new()),
        });
        let acceptor = {
            let state = Arc::clone(&state);
            std::thread::spawn(move || accept_loop(listener, state))
        };
        Self {
            address,
            state,
            acceptor: Some(acceptor),
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    /// `http://127.0.0.1:<port>`, without a trailing slash.
    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        lock(&self.state.requests).clone()
    }

    /// Recorded requests with this method and path.
    pub(crate) fn requests_to(&self, method: &str, path: &str) -> Vec<RecordedRequest> {
        self.requests()
            .into_iter()
            .filter(|request| request.method == method && request.path() == path)
            .collect()
    }

    pub(crate) fn count(&self, method: &str, path: &str) -> usize {
        self.requests_to(method, path).len()
    }

    pub(crate) fn outcomes(&self) -> Vec<StreamOutcome> {
        lock(&self.state.outcomes).clone()
    }

    /// Waits until `condition` holds over the recorded requests.
    pub(crate) fn wait_for(
        &self,
        timeout: Duration,
        condition: impl Fn(&[RecordedRequest]) -> bool,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if condition(&lock(&self.state.requests)) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// Shuts the server down and returns the stream outcomes, which are
    /// final once every connection thread has exited.
    pub(crate) fn shut_down(mut self) -> Vec<StreamOutcome> {
        self.stop();
        lock(&self.state.outcomes).clone()
    }

    fn stop(&mut self) {
        self.state.shutdown.store(true, Ordering::SeqCst);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        let connections = std::mem::take(&mut *lock(&self.state.connections));
        for (stream, _) in &connections {
            let _ = stream.shutdown(Shutdown::Both);
        }
        for (_, thread) in connections {
            let _ = thread.join();
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn accept_loop(listener: TcpListener, state: Arc<ServerState>) {
    while !state.shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_nodelay(true);
                let Ok(handle) = stream.try_clone() else {
                    continue;
                };
                let thread = {
                    let state = Arc::clone(&state);
                    std::thread::spawn(move || serve_connection(stream, state))
                };
                lock(&state.connections).push((handle, thread));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(1)),
        }
    }
}

fn serve_connection(stream: TcpStream, state: Arc<ServerState>) {
    let Ok(mut writer) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(stream);
    serve_requests(&mut reader, &mut writer, &state);
    // The server keeps a handle for shutdown, so dropping this thread's
    // handles alone would leave the connection open.
    let _ = writer.shutdown(Shutdown::Both);
}

fn serve_requests(reader: &mut BufReader<TcpStream>, writer: &mut TcpStream, state: &ServerState) {
    while let Some(request) = read_request(reader) {
        lock(&state.requests).push(request.clone());
        match state.script.reply(&request) {
            Reply::Respond(response) => {
                if !write_response(writer, &request, &response, state) {
                    return;
                }
            }
            Reply::CloseAfterStatusLine(status) => {
                let _ = write!(writer, "HTTP/1.1 {status} {}\r\n", reason(status));
                let _ = writer.flush();
                let _ = writer.shutdown(Shutdown::Both);
                return;
            }
            Reply::CloseAfterRequest => {
                let _ = writer.shutdown(Shutdown::Both);
                return;
            }
            Reply::NeverAnswer => {
                // Holds the connection until the peer closes it or the
                // server shuts down.
                let _ = reader
                    .get_ref()
                    .set_read_timeout(Some(Duration::from_millis(5)));
                let mut byte = [0u8; 1];
                while !state.shutdown.load(Ordering::SeqCst) {
                    match reader.read(&mut byte) {
                        Ok(0) => return,
                        Ok(_) => {}
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                            ) => {}
                        Err(_) => return,
                    }
                }
                return;
            }
        }
    }
}

/// Reads one request; `None` on EOF, a malformed request or a non-HTTP
/// first byte.
fn read_request(reader: &mut BufReader<TcpStream>) -> Option<RecordedRequest> {
    let first = reader.fill_buf().ok()?;
    if first.is_empty() || !first[0].is_ascii_alphabetic() {
        return None;
    }
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.trim_end().splitn(3, ' ');
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let mut headers = Vec::new();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).ok()? == 0 {
            return None;
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':')?;
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
    }
    let length = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .map_or(Some(0), |(_, value)| value.parse::<usize>().ok())?;
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).ok()?;
    Some(RecordedRequest {
        method,
        target,
        headers,
        body,
    })
}

/// Writes one response; returns whether the connection may serve another
/// request.
fn write_response(
    writer: &mut TcpStream,
    request: &RecordedRequest,
    response: &ScriptedResponse,
    state: &ServerState,
) -> bool {
    if !sleep_unless_shutdown(response.delay, state) {
        return false;
    }
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        response.status,
        reason(response.status)
    );
    for (name, value) in &response.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    match &response.body {
        BodyPlan::Fixed(body) => {
            head.push_str(&format!("content-length: {}\r\n\r\n", body.len()));
            let mut bytes = head.into_bytes();
            bytes.extend_from_slice(body);
            writer
                .write_all(&bytes)
                .and_then(|()| writer.flush())
                .is_ok()
        }
        BodyPlan::Chunked(chunks) => {
            head.push_str("transfer-encoding: chunked\r\n\r\n");
            if writer.write_all(head.as_bytes()).is_err() {
                return false;
            }
            for (delay, chunk) in chunks {
                if !sleep_unless_shutdown(*delay, state) {
                    return false;
                }
                let mut bytes = format!("{:x}\r\n", chunk.len()).into_bytes();
                bytes.extend_from_slice(chunk);
                bytes.extend_from_slice(b"\r\n");
                if writer
                    .write_all(&bytes)
                    .and_then(|()| writer.flush())
                    .is_err()
                {
                    return false;
                }
            }
            writer
                .write_all(b"0\r\n\r\n")
                .and_then(|()| writer.flush())
                .is_ok()
        }
        BodyPlan::Streamed { total, piece } => {
            head.push_str(&format!("content-length: {total}\r\n\r\n"));
            let mut written = 0;
            let mut complete = writer.write_all(head.as_bytes()).is_ok();
            let buffer = vec![b'x'; *piece];
            while complete && written < *total {
                let size = (*piece).min(*total - written);
                match writer.write_all(&buffer[..size]) {
                    Ok(()) => written += size,
                    Err(_) => complete = false,
                }
            }
            lock(&state.outcomes).push(StreamOutcome {
                target: request.target.clone(),
                written,
                total: *total,
                complete,
            });
            complete
        }
    }
}

fn sleep_unless_shutdown(delay: Duration, state: &ServerState) -> bool {
    let deadline = Instant::now() + delay;
    while Instant::now() < deadline {
        if state.shutdown.load(Ordering::SeqCst) {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1).min(deadline - Instant::now()));
    }
    !state.shutdown.load(Ordering::SeqCst)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        419 => "Authentication Timeout",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

/// Collects recorded requests by `METHOD path` for compact assertions.
pub(crate) fn request_lines(requests: &[RecordedRequest]) -> Vec<String> {
    requests
        .iter()
        .map(|request| format!("{} {}", request.method, request.path()))
        .collect()
}

/// Groups recorded requests by `METHOD path`, counting each.
pub(crate) fn request_counts(requests: &[RecordedRequest]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for line in request_lines(requests) {
        *counts.entry(line).or_insert(0) += 1;
    }
    counts
}

/// Waits on the async runtime until `condition` holds over the recorded
/// requests; panics after `timeout`.
pub(crate) async fn wait_until_requests(
    server: &TestServer,
    timeout: Duration,
    condition: impl Fn(&[RecordedRequest]) -> bool,
) {
    let deadline = Instant::now() + timeout;
    while !condition(&server.requests()) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for requests: {:?}",
            request_lines(&server.requests())
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// A frontend REST session composed as production composes it: the auth
/// owner injected into the real upstream `RestCatalog`, sharing one HTTP
/// client.
pub(crate) struct FrontendFixture {
    pub(crate) owner: Arc<super::auth::RestAuthOwner>,
    pub(crate) gate: super::attempt::SessionGate,
    pub(crate) upstream: Arc<crate::iceberg_catalog_rest::RestCatalog>,
}

impl FrontendFixture {
    /// `properties` is the persistent definition as handed to upstream
    /// (`uri` included).
    pub(crate) async fn new(
        properties: &[(&str, &str)],
        identity: Option<crate::access_binding::IcebergRestAuthMaterial>,
        http: super::http::RestHttpClient,
    ) -> Self {
        use crate::iceberg::CatalogBuilder;

        let properties: HashMap<String, String> = properties
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let persistent = super::auth::PersistentRestConfig::from_properties(&properties)
            .expect("valid persistent REST definition");
        let (owner, manager) =
            super::auth::RestAuthOwner::frontend(identity, persistent, http.clone())
                .expect("valid identity");
        let upstream = crate::iceberg_catalog_rest::RestCatalogBuilder::default()
            .with_client(http.reqwest_client().clone())
            .with_auth_manager(manager)
            .load("rest", properties)
            .await
            .expect("load upstream REST catalog");
        Self {
            owner,
            gate: super::attempt::SessionGate::default(),
            upstream: Arc::new(upstream),
        }
    }

    pub(crate) async fn ensure_session(
        &self,
    ) -> Result<Arc<super::binding::SessionBinding>, super::attempt::AttemptFailure> {
        super::attempt::ensure_session(&self.owner, &self.gate, &self.upstream).await
    }
}

/// An OAuth2 client-credential identity.
pub(crate) fn oauth2_identity(
    client_id: &str,
    client_secret: &str,
) -> crate::access_binding::IcebergRestAuthMaterial {
    crate::access_binding::IcebergRestAuthMaterial::Oauth2 {
        client_id: client_id.to_string(),
        client_secret: novarocks_fs::SecretValue::new(client_secret),
    }
}

/// A bearer-token identity.
pub(crate) fn bearer_identity(token: &str) -> crate::access_binding::IcebergRestAuthMaterial {
    crate::access_binding::IcebergRestAuthMaterial::Bearer {
        token: novarocks_fs::SecretValue::new(token),
    }
}
