package pipelite.executor.runner;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CommandRunnerUtilsTest {

  @Test
  public void testQuoteArgument() {
    assertThat(CommandRunnerUtils.quoteArgument("test")).isEqualTo("'test'");
    assertThat(CommandRunnerUtils.quoteArgument("'test'")).isEqualTo("'test'");
    assertThat(CommandRunnerUtils.quoteArgument("-i")).isEqualTo("-i");
  }

  @Test
  public void testQuoteArguments() {
    List<String> arguments = Arrays.asList("arg1", "arg2", "-i");
    assertThat(CommandRunnerUtils.quoteArguments(arguments)).isEqualTo("'arg1' 'arg2' -i");
  }
}
