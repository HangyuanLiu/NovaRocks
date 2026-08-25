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

#[path = "../publication_catalog.rs"]
mod publication_catalog;

use anyhow::Result;
use clap::Parser;
use publication_catalog::{FixtureConfig, serve};
use std::net::SocketAddr;

#[derive(Parser)]
#[command(about = "Test-only transparent Iceberg REST publication proxy")]
struct Args {
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long)]
    downstream: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    serve(FixtureConfig {
        listen: args.listen,
        downstream: args.downstream,
    })
    .await
}
