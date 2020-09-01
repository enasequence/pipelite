package pipelite.executor.call.utils;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.exec.util.StringUtils.isQuoted;

public class QuoteUtils {

  public static String quoteArguments(List<String> arguments) {
    return arguments.stream().map(arg -> quoteArgument(arg)).collect(Collectors.joining(" "));
  }

  public static String quoteArgument(String argument) {
    if (argument.startsWith("-") || isQuoted(argument)) {
      return argument;
    }
    return "'" + argument.replaceAll("'", "") + "'";
  }
}
