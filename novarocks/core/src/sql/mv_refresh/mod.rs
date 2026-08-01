// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! SQL-owned artifacts for materialized-view refresh.
//!
//! These artifacts describe the data plane only. They intentionally carry SQL
//! and immutable refresh facts, never result batches, catalog handles, or a
//! connector implementation.

pub(crate) mod first_refresh;
