// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

pub mod codec;
pub mod identity;
pub mod validation;

pub(crate) mod generated {
    include!(concat!(env!("OUT_DIR"), "/novarocks.mv.persistence.v1.rs"));
}
