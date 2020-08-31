package pipelite.executor.stream;

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
      super.write(b, off, buf.length - count > len ? len : buf.length - count);
    }
  }
}
