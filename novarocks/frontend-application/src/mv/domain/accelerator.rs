// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0.

//! Pure lake-to-Accelerator projection construction.
//!
//! The caller owns observation and CAS retry.  This module deliberately has no
//! StateStore access, so an obsolete observation cannot become durable after a
//! caller has observed a conflict.

use crate::mv::domain::storage_observation::MvLakePackageObservation;
use novarocks_mv_application::repository::MvProjectionRequest;

pub(crate) fn projection_from_lake(
    _package: &MvLakePackageObservation,
) -> Result<MvProjectionRequest, String> {
    Err(
        "legacy MV lake package cannot construct an Accelerator v2 projection without exact D/L/P/C revisions"
            .to_string(),
    )
}
