package pipelite.executor.executable.ssh;

import org.junit.jupiter.api.Test;
import pipelite.executor.executable.call.SshUtils;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SshUtilsTest {

  @Test
  public void testQuoteArgument() {
    assertThat(SshUtils.quoteArgument("test")).isEqualTo("'test'");
    assertThat(SshUtils.quoteArgument("'test'")).isEqualTo("'test'");
    assertThat(SshUtils.quoteArgument("-i")).isEqualTo("-i");
  }

  @Test
  public void testQuoteArguments() {
    List<String> arguments = Arrays.asList("arg1", "arg2", "-i");
    assertThat(SshUtils.quoteArguments(arguments)).isEqualTo("'arg1' 'arg2' -i");
  }
}
