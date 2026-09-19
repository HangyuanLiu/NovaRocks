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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

/** Test-only FileIO wrapper that counts operations actually delegated to the configured FileIO. */
public final class TracingFileIO implements FileIO {
  private static final long serialVersionUID = 1L;

  private final FileIO delegate;

  public TracingFileIO() {
    String kind = System.getenv().getOrDefault("UEA7_DELEGATE_FILE_IO", "hadoop");
    delegate =
        switch (kind) {
          case "hadoop" -> new HadoopFileIO();
          case "s3" -> new S3FileIO();
          default -> throw new IllegalArgumentException("unknown UEA7_DELEGATE_FILE_IO: " + kind);
        };
  }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate.initialize(properties);
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public InputFile newInputFile(String location) {
    FaultControlServer.inputFileCreated();
    return new TracingInputFile(delegate.newInputFile(location));
  }

  @Override
  public InputFile newInputFile(String location, long length) {
    FaultControlServer.inputFileCreated();
    return new TracingInputFile(delegate.newInputFile(location, length));
  }

  @Override
  public OutputFile newOutputFile(String location) {
    FaultControlServer.outputFileCreated();
    return new TracingOutputFile(delegate.newOutputFile(location));
  }

  @Override
  public void deleteFile(String location) {
    delegate.deleteFile(location);
    FaultControlServer.fileDeleted();
  }

  @Override
  public void close() {
    delegate.close();
  }

  private static final class TracingInputFile implements InputFile {
    private final InputFile delegate;

    private TracingInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      long length = delegate.getLength();
      FaultControlServer.inputFileLengthRead();
      return length;
    }

    @Override
    public SeekableInputStream newStream() {
      SeekableInputStream stream = delegate.newStream();
      FaultControlServer.inputStreamOpened();
      return new TracingSeekableInputStream(stream);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      boolean exists = delegate.exists();
      FaultControlServer.inputFileExistenceChecked();
      return exists;
    }
  }

  private static final class TracingOutputFile implements OutputFile {
    private final OutputFile delegate;

    private TracingOutputFile(OutputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public PositionOutputStream create() {
      PositionOutputStream stream = delegate.create();
      FaultControlServer.outputStreamOpened();
      return new TracingPositionOutputStream(stream);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      PositionOutputStream stream = delegate.createOrOverwrite();
      FaultControlServer.outputStreamOpened();
      return new TracingPositionOutputStream(stream);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public InputFile toInputFile() {
      FaultControlServer.inputFileCreated();
      return new TracingInputFile(delegate.toInputFile());
    }
  }

  private static final class TracingSeekableInputStream extends SeekableInputStream {
    private final SeekableInputStream delegate;

    private TracingSeekableInputStream(SeekableInputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public int read() throws IOException {
      int value = delegate.read();
      if (value >= 0) {
        FaultControlServer.inputBytesRead(1);
      }
      return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      int count = delegate.read(buffer, offset, length);
      FaultControlServer.inputBytesRead(count);
      return count;
    }

    @Override
    public long skip(long count) throws IOException {
      return delegate.skip(count);
    }

    @Override
    public int available() throws IOException {
      return delegate.available();
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long position) throws IOException {
      delegate.seek(position);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private static final class TracingPositionOutputStream extends PositionOutputStream {
    private final PositionOutputStream delegate;

    private TracingPositionOutputStream(PositionOutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(int value) throws IOException {
      delegate.write(value);
      FaultControlServer.outputBytesWritten(1);
    }

    @Override
    public void write(byte[] buffer, int offset, int length) throws IOException {
      delegate.write(buffer, offset, length);
      FaultControlServer.outputBytesWritten(length);
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public long storedLength() throws IOException {
      return delegate.storedLength();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
