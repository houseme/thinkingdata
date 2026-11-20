// Copyright 2026 houseme
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Helpers are compiled once per integration-test binary; each binary uses a
// different subset, so unused warnings are expected and silenced here.
#![allow(dead_code)]

//! A minimal HTTP mock server shared by the consumer integration tests.
//!
//! Every connection is answered with `Connection: close`, so each request is
//! independent. Responses are scripted as `(status, body)` pairs; when the
//! script is empty the server answers `200 {"code": 0}`.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One recorded HTTP request.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RecordedRequest {
    /// First header matching `name` (case-insensitive).
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
    }

    /// Body as a UTF-8 string.
    pub fn body_string(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// Raw (still percent-encoded) value of a form field.
    pub fn form_field_raw(&self, name: &str) -> Option<String> {
        let body = self.body_string();
        for pair in body.split('&') {
            let (key, value) = pair.split_once('=')?;
            if key == name {
                return Some(value.to_string());
            }
        }
        None
    }

    /// Percent-decoded value of a form field.
    pub fn form_field(&self, name: &str) -> Option<String> {
        self.form_field_raw(name)
            .map(|value| percent_decode(&value))
    }
}

/// Decode `%XX` escapes and `+` (as space) like `application/x-www-form-urlencoded`.
pub fn percent_decode(value: &str) -> String {
    let plus_fixed = value.replace('+', " ");
    let bytes = plus_fixed.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The shared mock receiver.
#[derive(Clone)]
pub struct MockServer {
    pub url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    script: Arc<Mutex<VecDeque<(u16, String)>>>,
}

impl MockServer {
    /// Start the server on an ephemeral local port.
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(VecDeque::new()));

        {
            let requests = requests.clone();
            let script = script.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { continue };
                    // Read on a clone so the original handle can write the response.
                    let request = stream.try_clone().ok().and_then(read_request);
                    if let Some(request) = request {
                        requests.lock().expect("requests lock").push(request);
                        let (status, body) = script
                            .lock()
                            .expect("script lock")
                            .pop_front()
                            .unwrap_or((200, "{\"code\":0,\"errorLevel\":0}".to_string()));
                        write_response(stream, status, &body);
                    }
                }
            });
        }

        Self {
            url,
            requests,
            script,
        }
    }

    /// Queue the next `(status, body)` response pair.
    pub fn script(&self, status: u16, body: &str) {
        self.script
            .lock()
            .expect("script lock")
            .push_back((status, body.to_string()));
    }

    /// Queue `count` copies of the same response.
    pub fn script_many(&self, count: usize, status: u16, body: &str) {
        for _ in 0..count {
            self.script(status, body);
        }
    }

    /// Snapshot of all recorded requests.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().expect("requests lock").clone()
    }

    /// Number of recorded requests so far.
    pub fn request_count(&self) -> usize {
        self.requests.lock().expect("requests lock").len()
    }

    /// Block until at least `at_least` requests have been recorded.
    ///
    /// The deadline is generous (30s): it only bites on a genuine failure,
    /// and a tight bound makes the suite flaky on heavily loaded machines.
    pub fn wait_requests(&self, at_least: usize) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.request_count() < at_least {
            if Instant::now() > deadline {
                panic!(
                    "timed out waiting for {at_least} requests; got {}",
                    self.request_count()
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// Read one HTTP/1.1 request; returns `None` on malformed input.
fn read_request(stream: std::net::TcpStream) -> Option<RecordedRequest> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let request_line = read_line(&mut reader)?;
    let mut parts = request_line.split_whitespace();
    let _method = parts.next()?;
    let path = parts.next()?.to_string();

    let mut headers = Vec::new();
    let mut content_length = 0usize;
    loop {
        let line = read_line(&mut reader)?;
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_string();
            let value = value.trim().to_string();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().unwrap_or(0);
            }
            headers.push((name, value));
        }
    }

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).ok()?;
    }
    Some(RecordedRequest {
        path,
        headers,
        body,
    })
}

fn read_line(reader: &mut BufReader<std::net::TcpStream>) -> Option<String> {
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    Some(line.trim_end_matches(['\r', '\n']).to_string())
}

fn write_response(mut stream: std::net::TcpStream, status: u16, body: &str) {
    let reason = match status {
        200 => "OK",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}
