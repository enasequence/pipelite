package pipelite.executor;

import java.io.ByteArrayOutputStream;

public class ConstrainedByteArrayOutputStream extends ByteArrayOutputStream {

  public ConstrainedByteArrayOutputStream(int maxSize) {
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
