package pipelite.executor.output;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KeepOldestByteArrayOutputStreamTest {

  @Test
  public void test() {
    KeepOldestByteArrayOutputStream strm = new KeepOldestByteArrayOutputStream(5);
    strm.write(new byte[] {'a'}, 0, 1);
    assertThat(strm.toString()).isEqualTo("a");
    strm.write(new byte[] {'b', 'c'}, 0, 2);
    assertThat(strm.toString()).isEqualTo("abc");
    strm.write(new byte[] {'d', 'e', 'f', 'g', 'i', 'j', 'k'}, 0, 8);
    assertThat(strm.toString()).isEqualTo("abcde");
  }
}
