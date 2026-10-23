// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::atomic::{AtomicUsize, Ordering};

use http_body::Body;

use super::*;

struct OriginalCustody(Arc<AtomicUsize>);
impl Drop for OriginalCustody {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn queued_upload_frames_keep_original_custody_through_body_drop_clones_and_slices() {
    let exited = Arc::new(AtomicUsize::new(0));
    let custody = Arc::new(OriginalCustody(Arc::clone(&exited)));
    let input = Buffer::from(vec![
        Bytes::from_static(b"first"),
        Bytes::from_static(b"second"),
    ]);
    let mut body = HttpBufferBody::retained(input, Arc::clone(&custody));
    drop(custody);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut frame = || match Pin::new(&mut body).poll_frame(&mut cx) {
        Poll::Ready(Some(Ok(frame))) => frame.into_data().unwrap(),
        _ => panic!("upload body did not yield its original data"),
    };
    let first = frame();
    let second = frame();
    assert_eq!(first.as_ref(), b"first");
    assert_eq!(second.as_ref(), b"second");
    let clone = first.clone();
    let slice = second.slice(1..4);
    drop(body);
    assert_eq!(exited.load(Ordering::SeqCst), 0);
    drop(first);
    drop(second);
    assert_eq!(exited.load(Ordering::SeqCst), 0);
    drop(clone);
    assert_eq!(exited.load(Ordering::SeqCst), 0);
    assert_eq!(slice.as_ref(), b"eco");
    drop(slice);
    assert_eq!(exited.load(Ordering::SeqCst), 1);
}
