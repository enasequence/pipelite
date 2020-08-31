package pipelite.executor.stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KeepNewestByteArrayOutputStreamTest {

  @Test
  public void test() {
    KeepNewestByteArrayOutputStream strm = new KeepNewestByteArrayOutputStream(5);
    strm.write(new byte[] {'a'}, 0, 1);
    assertThat(strm.toString()).isEqualTo("a");
    strm.write(new byte[] {'b', 'c'}, 0, 2);
    assertThat(strm.toString()).isEqualTo("abc");
    strm.write(new byte[] {'d'}, 0, 1);
    assertThat(strm.toString()).isEqualTo("abcd");
    strm.write(new byte[] {'e'}, 0, 1);
    assertThat(strm.toString()).isEqualTo("abcde");
    strm.write(new byte[] {'f'}, 0, 1);
    assertThat(strm.toString()).isEqualTo("bcdef");
    strm.write(new byte[] {'1', '2', '3', '4'}, 0, 4);
    assertThat(strm.toString()).isEqualTo("f1234");
  }
}
