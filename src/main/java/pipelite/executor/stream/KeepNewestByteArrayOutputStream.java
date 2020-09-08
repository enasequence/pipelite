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
package pipelite.executor.stream;

import java.io.ByteArrayOutputStream;

public class KeepNewestByteArrayOutputStream extends ByteArrayOutputStream {

  public static final int DEFAULT_MAX_BYTES = 1024 * 1024;

  private static final int BUFFER_OVERWRITE_PERCENTAGE = 20;

  public KeepNewestByteArrayOutputStream() {
    super(DEFAULT_MAX_BYTES);
  }

  public KeepNewestByteArrayOutputStream(int maxSize) {
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
    if (count + len > buf.length * (100 - BUFFER_OVERWRITE_PERCENTAGE) / 100) {
      // Move buffer content to make space for new output. Old buffer
      // content will be overwritten by new buffer content.
      while (buf.length - count < len && count > 0) {
        int r = buf.length * BUFFER_OVERWRITE_PERCENTAGE / 100;
        if (count - r < 0) {
          r = count;
        }
        System.arraycopy(buf, r, buf, 0, count - r);
        count -= r;
      }
    }

    super.write(b, off, len);
  }
}
