-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

DROP TABLE IF EXISTS ss3_probe_keys;
DROP TABLE IF EXISTS ss3_probe_snapshot;
DROP TABLE IF EXISTS ss3_probe_locks;

CREATE TABLE ss3_probe_keys (
    key_bytes VARBINARY(3072) NOT NULL,
    value_bytes VARBINARY(64) NOT NULL,
    PRIMARY KEY (key_bytes)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE ss3_probe_snapshot (
    id INT NOT NULL,
    value_bytes INT NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
CREATE TABLE ss3_probe_locks (
    id INT NOT NULL,
    value_bytes INT NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
