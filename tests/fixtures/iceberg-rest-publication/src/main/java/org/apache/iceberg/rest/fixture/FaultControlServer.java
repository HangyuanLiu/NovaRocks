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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;

final class FaultControlServer {
  private static final Object LOCK = new Object();
  private static final int MAX_TRACE_EVENTS = 512;
  private static final int MAX_PARAMETER_LENGTH = 512;
  private static final AtomicBoolean STARTED = new AtomicBoolean(false);
  private static final AtomicLong ARM_SEQUENCE = new AtomicLong(1);
  private static final AtomicLong COMMIT_SEQUENCE = new AtomicLong(1);
  private static final AtomicLong COMMIT_ATTEMPTS = new AtomicLong();
  private static final AtomicLong COMMIT_SUCCESSES = new AtomicLong();
  private static final AtomicLong COMMIT_CONFLICTS = new AtomicLong();
  private static final AtomicLong COMMIT_FAILURES = new AtomicLong();
  private static final AtomicLong INPUT_FILES = new AtomicLong();
  private static final AtomicLong INPUT_FILE_LENGTH_CALLS = new AtomicLong();
  private static final AtomicLong INPUT_FILE_EXISTS_CALLS = new AtomicLong();
  private static final AtomicLong INPUT_STREAMS = new AtomicLong();
  private static final AtomicLong INPUT_BYTES = new AtomicLong();
  private static final AtomicLong OUTPUT_FILES = new AtomicLong();
  private static final AtomicLong OUTPUT_STREAMS = new AtomicLong();
  private static final AtomicLong OUTPUT_BYTES = new AtomicLong();
  private static final AtomicLong DELETE_SUCCESSES = new AtomicLong();
  private static final Deque<TraceEvent> TRACE = new ArrayDeque<>();
  private static ActiveHold activeHold;
  private static long maxHoldSeconds;
  private static long traceSequence = 1;

  private FaultControlServer() {}

  static void startFromEnvironment() {
    if (!STARTED.compareAndSet(false, true)) {
      return;
    }

    int port = parseBoundedInteger("UEA7_FAULT_CONTROL_PORT", 8182, 1, 65535);
    maxHoldSeconds =
        parseBoundedInteger("UEA7_FAULT_MAX_HOLD_SECONDS", 120, 1, 600);
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 8);
      server.createContext("/health", FaultControlServer::health);
      server.createContext("/arm", FaultControlServer::arm);
      server.createContext("/status", FaultControlServer::status);
      server.createContext("/release", FaultControlServer::release);
      server.createContext("/trace", FaultControlServer::trace);
      server.createContext("/metrics", FaultControlServer::metrics);
      server.setExecutor(
          Executors.newFixedThreadPool(
              2,
              runnable -> {
                Thread thread = new Thread(runnable, "uea7-fault-control");
                thread.setDaemon(true);
                return thread;
              }));
      server.start();
      record("control-started", "", "", "", "", "", "port=" + port);
    } catch (IOException exception) {
      STARTED.set(false);
      throw new IllegalStateException("failed to start UEA-7 fault control server", exception);
    }
  }

  static CommitAttempt beforePersistentCommit(
      String table, TableMetadata base, TableMetadata updated) {
    COMMIT_ATTEMPTS.incrementAndGet();
    String commitId = "commit-" + COMMIT_SEQUENCE.getAndIncrement();
    String baseIdentity = metadataIdentity(base);
    String updatedIdentity = metadataIdentity(updated);
    ActiveHold hold = null;

    synchronized (LOCK) {
      if (activeHold != null
          && activeHold.phase == HoldPhase.ARMED
          && activeHold.table.equals(table)) {
        hold = activeHold;
        hold.phase = HoldPhase.HELD;
        hold.commitId = commitId;
        hold.thread = Thread.currentThread().getName();
        record(
            "requirements-passed-before-persistent-commit",
            table,
            hold.armId,
            commitId,
            baseIdentity,
            updatedIdentity,
            "TableOperations.commit entry");
        record(
            "hold-reached",
            table,
            hold.armId,
            commitId,
            baseIdentity,
            updatedIdentity,
            "no delegate commit has started");
      }
    }

    String armId = hold == null ? "" : hold.armId;
    if (hold == null) {
      record(
          "requirements-passed-before-persistent-commit",
          table,
          armId,
          commitId,
          baseIdentity,
          updatedIdentity,
          "TableOperations.commit entry");
    } else {
      try {
        if (!hold.released.await(maxHoldSeconds, TimeUnit.SECONDS)) {
          synchronized (LOCK) {
            hold.phase = HoldPhase.TIMED_OUT;
          }
          record(
              "hold-timed-out",
              table,
              armId,
              commitId,
              baseIdentity,
              updatedIdentity,
              "bounded hold expired");
          throw new IllegalStateException("UEA-7 publication hold timed out");
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        synchronized (LOCK) {
          hold.phase = HoldPhase.FAILED;
        }
        record(
            "hold-interrupted",
            table,
            armId,
            commitId,
            baseIdentity,
            updatedIdentity,
            exception.toString());
        throw new IllegalStateException("UEA-7 publication hold was interrupted", exception);
      }
    }

    record(
        "delegate-commit-start",
        table,
        armId,
        commitId,
        baseIdentity,
        updatedIdentity,
        "continuing the original base and updated metadata");
    return new CommitAttempt(commitId, armId, baseIdentity, updatedIdentity, hold);
  }

  static void commitSucceeded(String table, CommitAttempt attempt) {
    COMMIT_SUCCESSES.incrementAndGet();
    record(
        "delegate-commit-success",
        table,
        attempt.armId,
        attempt.commitId,
        attempt.baseIdentity,
        attempt.updatedIdentity,
        "");
    finishHold(attempt.hold, HoldPhase.SUCCEEDED);
  }

  static void commitFailed(String table, CommitAttempt attempt, RuntimeException failure) {
    boolean conflict = failure instanceof CommitFailedException;
    (conflict ? COMMIT_CONFLICTS : COMMIT_FAILURES).incrementAndGet();
    record(
        conflict ? "delegate-commit-conflict" : "delegate-commit-failed",
        table,
        attempt.armId,
        attempt.commitId,
        attempt.baseIdentity,
        attempt.updatedIdentity,
        failure.toString());
    finishHold(attempt.hold, conflict ? HoldPhase.CONFLICT : HoldPhase.FAILED);
  }

  static void refreshed(String table, TableMetadata metadata) {
    record("refresh", table, "", "", metadataIdentity(metadata), "", "");
  }

  static void inputFileCreated() {
    INPUT_FILES.incrementAndGet();
  }

  static void inputFileLengthRead() {
    INPUT_FILE_LENGTH_CALLS.incrementAndGet();
  }

  static void inputFileExistenceChecked() {
    INPUT_FILE_EXISTS_CALLS.incrementAndGet();
  }

  static void inputStreamOpened() {
    INPUT_STREAMS.incrementAndGet();
  }

  static void inputBytesRead(long count) {
    if (count > 0) {
      INPUT_BYTES.addAndGet(count);
    }
  }

  static void outputFileCreated() {
    OUTPUT_FILES.incrementAndGet();
  }

  static void outputStreamOpened() {
    OUTPUT_STREAMS.incrementAndGet();
  }

  static void outputBytesWritten(long count) {
    if (count > 0) {
      OUTPUT_BYTES.addAndGet(count);
    }
  }

  static void fileDeleted() {
    DELETE_SUCCESSES.incrementAndGet();
  }

  private static void finishHold(ActiveHold hold, HoldPhase phase) {
    if (hold == null) {
      return;
    }
    synchronized (LOCK) {
      hold.phase = phase;
    }
  }

  private static void health(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      send(exchange, 405, "{\"error\":\"method not allowed\"}\n");
      return;
    }
    send(exchange, 200, "{\"status\":\"ok\"}\n");
  }

  private static void arm(HttpExchange exchange) throws IOException {
    if (!"POST".equals(exchange.getRequestMethod())) {
      send(exchange, 405, "{\"error\":\"method not allowed\"}\n");
      return;
    }
    String table;
    try {
      table = requiredParameter(exchange, "table");
    } catch (IllegalArgumentException exception) {
      send(exchange, 400, errorJson(exception.getMessage()));
      return;
    }

    ActiveHold hold;
    synchronized (LOCK) {
      if (activeHold != null && !activeHold.phase.isTerminal()) {
        send(exchange, 409, errorJson("a publication hold is already active"));
        return;
      }
      hold = new ActiveHold("arm-" + ARM_SEQUENCE.getAndIncrement(), table);
      activeHold = hold;
      record("hold-armed", table, hold.armId, "", "", "", "");
    }
    send(
        exchange,
        200,
        "{\"arm_id\":" + quoted(hold.armId) + ",\"phase\":\"armed\"}\n");
  }

  private static void status(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      send(exchange, 405, "{\"error\":\"method not allowed\"}\n");
      return;
    }
    String armId;
    try {
      armId = requiredParameter(exchange, "arm_id");
    } catch (IllegalArgumentException exception) {
      send(exchange, 400, errorJson(exception.getMessage()));
      return;
    }

    synchronized (LOCK) {
      if (activeHold == null || !activeHold.armId.equals(armId)) {
        send(exchange, 404, errorJson("unknown arm_id"));
        return;
      }
      send(exchange, 200, activeHold.toJson());
    }
  }

  private static void release(HttpExchange exchange) throws IOException {
    if (!"POST".equals(exchange.getRequestMethod())) {
      send(exchange, 405, "{\"error\":\"method not allowed\"}\n");
      return;
    }
    String armId;
    try {
      armId = requiredParameter(exchange, "arm_id");
    } catch (IllegalArgumentException exception) {
      send(exchange, 400, errorJson(exception.getMessage()));
      return;
    }

    ActiveHold hold;
    synchronized (LOCK) {
      if (activeHold == null || !activeHold.armId.equals(armId)) {
        send(exchange, 404, errorJson("unknown arm_id"));
        return;
      }
      hold = activeHold;
      if (hold.phase != HoldPhase.HELD) {
        send(exchange, 409, errorJson("hold has not reached the persistent commit boundary"));
        return;
      }
      hold.phase = HoldPhase.RELEASED;
      record("hold-released", hold.table, hold.armId, hold.commitId, "", "", "");
      hold.released.countDown();
    }
    send(exchange, 200, hold.toJson());
  }

  private static void trace(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      send(exchange, 405, "{\"error\":\"method not allowed\"}\n");
      return;
    }
    List<TraceEvent> snapshot;
    synchronized (LOCK) {
      snapshot = new ArrayList<>(TRACE);
    }
    StringBuilder body = new StringBuilder(snapshot.size() * 256);
    for (TraceEvent event : snapshot) {
      body.append(event.toJson()).append('\n');
    }
    send(exchange, 200, body.toString(), "application/x-ndjson; charset=utf-8");
  }

  private static void metrics(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      send(exchange, 405, "{\"error\":\"method not allowed\"}\n");
      return;
    }
    send(
        exchange,
        200,
        "{"
            + "\"commit_attempts\":"
            + COMMIT_ATTEMPTS.get()
            + ",\"commit_successes\":"
            + COMMIT_SUCCESSES.get()
            + ",\"commit_conflicts\":"
            + COMMIT_CONFLICTS.get()
            + ",\"commit_failures\":"
            + COMMIT_FAILURES.get()
            + ",\"input_files\":"
            + INPUT_FILES.get()
            + ",\"input_file_length_calls\":"
            + INPUT_FILE_LENGTH_CALLS.get()
            + ",\"input_file_exists_calls\":"
            + INPUT_FILE_EXISTS_CALLS.get()
            + ",\"input_streams\":"
            + INPUT_STREAMS.get()
            + ",\"input_bytes\":"
            + INPUT_BYTES.get()
            + ",\"output_files\":"
            + OUTPUT_FILES.get()
            + ",\"output_streams\":"
            + OUTPUT_STREAMS.get()
            + ",\"output_bytes\":"
            + OUTPUT_BYTES.get()
            + ",\"delete_successes\":"
            + DELETE_SUCCESSES.get()
            + "}\n");
  }

  private static void record(
      String event,
      String table,
      String armId,
      String commitId,
      String baseMetadata,
      String updatedMetadata,
      String message) {
    synchronized (LOCK) {
      TraceEvent traceEvent =
          new TraceEvent(
              traceSequence++,
              event,
              table,
              armId,
              commitId,
              baseMetadata,
              updatedMetadata,
              message);
      while (TRACE.size() >= MAX_TRACE_EVENTS) {
        TRACE.removeFirst();
      }
      TRACE.addLast(traceEvent);
    }
  }

  private static String requiredParameter(HttpExchange exchange, String name) {
    Map<String, String> parameters = parseQuery(exchange.getRequestURI().getRawQuery());
    String value = parameters.get(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("missing query parameter: " + name);
    }
    if (value.length() > MAX_PARAMETER_LENGTH) {
      throw new IllegalArgumentException("query parameter exceeds the fixture limit: " + name);
    }
    return value;
  }

  private static Map<String, String> parseQuery(String query) {
    Map<String, String> result = new HashMap<>();
    if (query == null || query.isEmpty()) {
      return result;
    }
    for (String pair : query.split("&", 32)) {
      String[] parts = pair.split("=", 2);
      String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
      String value =
          parts.length == 2 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
      result.putIfAbsent(key, value);
    }
    return result;
  }

  private static int parseBoundedInteger(String name, int defaultValue, int minimum, int maximum) {
    String raw = System.getenv(name);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      int value = Integer.parseInt(raw);
      if (value < minimum || value > maximum) {
        throw new IllegalArgumentException(name + " is outside the supported range");
      }
      return value;
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(name + " is not an integer", exception);
    }
  }

  private static String metadataIdentity(TableMetadata metadata) {
    if (metadata == null) {
      return "null";
    }
    return metadata.uuid()
        + "|"
        + metadata.metadataFileLocation()
        + "|"
        + metadata.schema().schemaId()
        + "|"
        + metadata.lastColumnId();
  }

  private static void send(HttpExchange exchange, int status, String body) throws IOException {
    send(exchange, status, body, "application/json; charset=utf-8");
  }

  private static void send(HttpExchange exchange, int status, String body, String contentType)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private static String errorJson(String message) {
    return "{\"error\":" + quoted(message) + "}\n";
  }

  private static String quoted(String value) {
    String escaped =
        value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r");
    return "\"" + escaped + "\"";
  }

  static final class CommitAttempt {
    private final String commitId;
    private final String armId;
    private final String baseIdentity;
    private final String updatedIdentity;
    private final ActiveHold hold;

    private CommitAttempt(
        String commitId,
        String armId,
        String baseIdentity,
        String updatedIdentity,
        ActiveHold hold) {
      this.commitId = commitId;
      this.armId = armId;
      this.baseIdentity = baseIdentity;
      this.updatedIdentity = updatedIdentity;
      this.hold = hold;
    }
  }

  private enum HoldPhase {
    ARMED,
    HELD,
    RELEASED,
    SUCCEEDED,
    CONFLICT,
    FAILED,
    TIMED_OUT;

    private boolean isTerminal() {
      return this == SUCCEEDED || this == CONFLICT || this == FAILED || this == TIMED_OUT;
    }

    private String wireName() {
      return name().toLowerCase().replace('_', '-');
    }
  }

  private static final class ActiveHold {
    private final String armId;
    private final String table;
    private final CountDownLatch released = new CountDownLatch(1);
    private HoldPhase phase = HoldPhase.ARMED;
    private String commitId = "";
    private String thread = "";

    private ActiveHold(String armId, String table) {
      this.armId = armId;
      this.table = table;
    }

    private String toJson() {
      return "{\"arm_id\":"
          + quoted(armId)
          + ",\"table\":"
          + quoted(table)
          + ",\"phase\":"
          + quoted(phase.wireName())
          + ",\"commit_id\":"
          + quoted(commitId)
          + ",\"thread\":"
          + quoted(thread)
          + "}\n";
    }
  }
}
