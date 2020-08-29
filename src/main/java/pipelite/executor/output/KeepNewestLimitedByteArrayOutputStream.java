package pipelite.executor.output;

import java.io.ByteArrayOutputStream;

public class KeepNewestLimitedByteArrayOutputStream extends ByteArrayOutputStream {

  public static final int DEFAULT_MAX_BYTES = 1024 * 1024;

  private static final int BUFFER_OVERWRITE_PERCENTAGE = 25;

  public KeepNewestLimitedByteArrayOutputStream() {
    super(DEFAULT_MAX_BYTES);
  }

  public KeepNewestLimitedByteArrayOutputStream(int maxSize) {
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
      int r = buf.length * BUFFER_OVERWRITE_PERCENTAGE / 100;
      System.arraycopy(buf, r, buf, 0, count - r);
      count -= r;
    }

    super.write(b, off, len);
  }
}
