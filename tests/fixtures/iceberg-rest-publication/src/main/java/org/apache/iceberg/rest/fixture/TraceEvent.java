/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest.fixture;

final class TraceEvent {
  private static final int MAX_VALUE_LENGTH = 512;

  private final long sequence;
  private final long timestampMillis;
  private final String event;
  private final String table;
  private final String armId;
  private final String commitId;
  private final String baseMetadata;
  private final String updatedMetadata;
  private final String thread;
  private final String message;

  TraceEvent(
      long sequence,
      String event,
      String table,
      String armId,
      String commitId,
      String baseMetadata,
      String updatedMetadata,
      String message) {
    this.sequence = sequence;
    this.timestampMillis = System.currentTimeMillis();
    this.event = bounded(event);
    this.table = bounded(table);
    this.armId = bounded(armId);
    this.commitId = bounded(commitId);
    this.baseMetadata = bounded(baseMetadata);
    this.updatedMetadata = bounded(updatedMetadata);
    this.thread = bounded(Thread.currentThread().getName());
    this.message = bounded(message);
  }

  String toJson() {
    return "{"
        + "\"sequence\":"
        + sequence
        + ",\"timestamp_ms\":"
        + timestampMillis
        + ",\"event\":"
        + quoted(event)
        + ",\"table\":"
        + quoted(table)
        + ",\"arm_id\":"
        + quoted(armId)
        + ",\"commit_id\":"
        + quoted(commitId)
        + ",\"base_metadata\":"
        + quoted(baseMetadata)
        + ",\"updated_metadata\":"
        + quoted(updatedMetadata)
        + ",\"thread\":"
        + quoted(thread)
        + ",\"message\":"
        + quoted(message)
        + "}";
  }

  private static String bounded(String value) {
    if (value == null) {
      return "";
    }
    return value.length() <= MAX_VALUE_LENGTH ? value : value.substring(0, MAX_VALUE_LENGTH);
  }

  private static String quoted(String value) {
    StringBuilder result = new StringBuilder(value.length() + 2);
    result.append('"');
    for (int index = 0; index < value.length(); index += 1) {
      char character = value.charAt(index);
      switch (character) {
        case '"':
          result.append("\\\"");
          break;
        case '\\':
          result.append("\\\\");
          break;
        case '\n':
          result.append("\\n");
          break;
        case '\r':
          result.append("\\r");
          break;
        case '\t':
          result.append("\\t");
          break;
        default:
          if (character < 0x20) {
            result.append(String.format("\\u%04x", (int) character));
          } else {
            result.append(character);
          }
      }
    }
    result.append('"');
    return result.toString();
  }
}
