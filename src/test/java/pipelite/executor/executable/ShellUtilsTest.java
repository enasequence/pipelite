package pipelite.executor.executable;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ShellUtilsTest {

  @Test
  public void testQuoteArgument() {
    assertThat(ShellUtils.quoteArgument("test")).isEqualTo("'test'");
    assertThat(ShellUtils.quoteArgument("'test'")).isEqualTo("'test'");
    assertThat(ShellUtils.quoteArgument("-i")).isEqualTo("-i");
  }

  @Test
  public void testQuoteArguments() {
    List<String> arguments = Arrays.asList("arg1", "arg2", "-i");
    assertThat(ShellUtils.quoteArguments(arguments)).isEqualTo("'arg1' 'arg2' -i");
  }
}
