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

//! One original absolute deadline on the existing client socket, with an owned joined helper.
use std::io;
use std::net::{Shutdown, TcpStream};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;
use std::time::Instant;

pub struct SocketDeadlineOwner {
    socket: TcpStream,
    deadline: Instant,
    cancel: Option<Sender<()>>,
    helper: Option<JoinHandle<io::Result<()>>>,
    failure: Option<String>,
}
impl SocketDeadlineOwner {
    pub fn duplicate(connection: &impl AsRawFd, deadline: Instant) -> io::Result<Self> {
        // SAFETY: the referenced live connection owns this descriptor throughout
        // the immediate duplication. The returned OwnedFd owns a separate alias;
        // no helper ever stores or takes ownership of the borrowed descriptor.
        let descriptor =
            unsafe { BorrowedFd::borrow_raw(connection.as_raw_fd()) }.try_clone_to_owned()?;
        Self::from_socket(TcpStream::from(descriptor), deadline)
    }
    fn from_socket(socket: TcpStream, deadline: Instant) -> io::Result<Self> {
        let mut owner = Self {
            socket,
            deadline,
            cancel: None,
            helper: None,
            failure: None,
        };
        owner.before_operation()?;
        let watched = owner.socket.try_clone()?;
        let (cancel, receiver) = mpsc::channel();
        let helper = match std::thread::Builder::new()
            .name("test-client-socket-deadline".into())
            .spawn(move || {
                match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => Ok(()),
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        let stopped = watched.shutdown(Shutdown::Both);
                        Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            match stopped {
                                Ok(()) => "Test client original socket deadline expired".into(),
                                Err(error) if error.kind() == io::ErrorKind::NotConnected => {
                                    "Test client original socket deadline expired".into()
                                }
                                Err(error) => format!(
                                    "Test client original socket deadline expired; shutdown: {error}"
                                ),
                            },
                        ))
                    }
                }
            }) {
            Ok(helper) => helper,
            Err(primary) => {
                let shutdown = owner.socket.shutdown(Shutdown::Both);
                return Err(io::Error::other(format!(
                    "start Test socket deadline helper: {primary}; socket shutdown={shutdown:?}"
                )));
            }
        };
        owner.cancel = Some(cancel);
        owner.helper = Some(helper);
        Ok(owner)
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn before_operation(&self) -> io::Result<()> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let shutdown = self.socket.shutdown(Shutdown::Both);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("Test client original deadline expired; socket shutdown={shutdown:?}"),
            ));
        }
        // Socket options affect the same original connection, with no proxy,
        // replacement statement generation or packet buffering.
        self.socket.set_read_timeout(Some(remaining))?;
        self.socket.set_write_timeout(Some(remaining))
    }
    /// Join after the protected operation has actually returned. A session-wide
    /// owner stays alive through client Drop; a shorter operation owner
    /// may join earlier while that original session owner remains in place.
    pub fn close_and_join(&mut self) -> io::Result<()> {
        // SO_RCVTIMEO may return just before the watchdog is scheduled. Closing
        // after the original deadline still fails; a racing cancellation cannot
        // turn an expired request into successful cleanup evidence.
        if self.helper.is_some() && Instant::now() >= self.deadline && self.failure.is_none() {
            self.failure = Some("Test client original socket deadline expired".into());
        }
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(helper) = self.helper.take() {
            match helper.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => self.failure = Some(error.to_string()),
                Err(_) => {
                    self.failure = Some(
                        "Test client socket deadline helper panicked during actual join".into(),
                    )
                }
            }
        }
        match &self.failure {
            Some(failure) => Err(io::Error::other(failure.clone())),
            None => Ok(()),
        }
    }
}
impl Drop for SocketDeadlineOwner {
    fn drop(&mut self) {
        if let Err(error) = self.close_and_join() {
            eprintln!("Test client deadline owner cleanup failed: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;
    fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }
    #[test]
    fn original_socket_wait_is_cut_off_and_the_same_helper_is_actually_joined() {
        let (mut client, _server) = pair();
        let deadline = Instant::now() + Duration::from_millis(80);
        let mut owner = SocketDeadlineOwner::duplicate(&client, deadline).unwrap();
        assert_eq!(
            client.local_addr().unwrap(),
            owner.socket.local_addr().unwrap()
        );
        let mut byte = [0];
        let result = client.read(&mut byte);
        assert!(matches!(result, Ok(0) | Err(_)));
        assert!(Instant::now() >= deadline);
        drop(client);
        assert!(owner.close_and_join().is_err());
        assert!(owner.helper.is_none());
        assert!(owner.close_and_join().is_err());
    }
    #[test]
    fn partial_progress_and_new_operations_do_not_renew_the_original_deadline() {
        let (mut client, mut server) = pair();
        let began = Instant::now();
        let deadline = began + Duration::from_millis(100);
        let mut owner = SocketDeadlineOwner::duplicate(&client, deadline).unwrap();
        for _ in 0..3 {
            server.write_all(b"x").unwrap();
            owner.before_operation().unwrap();
            client.read_exact(&mut [0]).unwrap();
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(owner.deadline(), deadline);
        let _ = client.read(&mut [0]);
        assert!(began.elapsed() < Duration::from_secs(2));
        drop(client);
        assert!(owner.close_and_join().is_err());
    }
    #[test]
    fn cancelling_and_joining_the_duplicate_never_takes_ownership_of_the_original_fd() {
        let (mut client, mut server) = pair();
        let mut owner =
            SocketDeadlineOwner::duplicate(&client, Instant::now() + Duration::from_secs(2))
                .unwrap();
        owner.close_and_join().unwrap();
        drop(owner);
        server.write_all(b"original").unwrap();
        let mut bytes = [0; 8];
        client.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"original");
    }
}
