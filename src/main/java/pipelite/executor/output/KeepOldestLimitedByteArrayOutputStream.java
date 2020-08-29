package pipelite.executor.output;

import java.io.ByteArrayOutputStream;

public class KeepOldestLimitedByteArrayOutputStream extends ByteArrayOutputStream {

  public static final int DEFAULT_MAX_BYTES = 1024 * 1024;

  public KeepOldestLimitedByteArrayOutputStream() {
    super(DEFAULT_MAX_BYTES);
  }

  public KeepOldestLimitedByteArrayOutputStream(int maxSize) {
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
      super.write(b, off, buf.length - count > len ? len : buf.length - count);
    }
  }
}
