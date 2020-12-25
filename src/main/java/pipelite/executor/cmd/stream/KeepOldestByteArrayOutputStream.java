/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor.cmd.stream;

import java.io.ByteArrayOutputStream;

public class KeepOldestByteArrayOutputStream extends ByteArrayOutputStream {

  public static final int DEFAULT_MAX_BYTES = 1024 * 1024;

  public KeepOldestByteArrayOutputStream() {
    super(DEFAULT_MAX_BYTES);
  }

  public KeepOldestByteArrayOutputStream(int maxSize) {
    super(maxSize);
  }

  @Override
  public synchronized void write(int b) {
    if (count < buf.length) {
      super.write(b);
    }
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) {
    if (count < buf.length) {
      super.write(b, off, Math.min(buf.length - count, len));
    }
  }
}
