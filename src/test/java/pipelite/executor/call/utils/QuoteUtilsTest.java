package pipelite.executor.call.utils;

import org.junit.jupiter.api.Test;
import pipelite.executor.call.utils.QuoteUtils;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class QuoteUtilsTest {

  @Test
  public void testQuoteArgument() {
    assertThat(QuoteUtils.quoteArgument("test")).isEqualTo("'test'");
    assertThat(QuoteUtils.quoteArgument("'test'")).isEqualTo("'test'");
    assertThat(QuoteUtils.quoteArgument("-i")).isEqualTo("-i");
  }

  @Test
  public void testQuoteArguments() {
    List<String> arguments = Arrays.asList("arg1", "arg2", "-i");
    assertThat(QuoteUtils.quoteArguments(arguments)).isEqualTo("'arg1' 'arg2' -i");
  }
}
