package pipelite.executor;

import pipelite.instance.TaskInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CallUtils {
  private CallUtils() {}

  public static List<String> getInternalTaskExecutorArgs(TaskInstance taskInstance) {
    List<String> args = new ArrayList<>();

    args.addAll(taskInstance.getTaskParameters().getEnvAsJavaSystemPropertyOptions());

    Integer memory = taskInstance.getTaskParameters().getMemory();

    if (memory != null && memory > 0) {
      args.add(String.format("-Xmx%dM", memory));
    }

    args.add("-cp");
    args.add(System.getProperty("java.class.path"));
    args.add(InternalTaskExecutor.class.getName());

    List<String> internalTaskExecutorArgs = new ArrayList<>();
    internalTaskExecutorArgs.add(taskInstance.getProcessName());
    internalTaskExecutorArgs.add(taskInstance.getProcessId());
    internalTaskExecutorArgs.add(taskInstance.getTaskName());
    internalTaskExecutorArgs.add(taskInstance.getExecutor().getClass().getName());
    internalTaskExecutorArgs.add(taskInstance.getResolver().getClass().getName());

    args.addAll(CallUtils.quoteArguments(internalTaskExecutorArgs));

    return args;
  }

  public static List<String> quoteArguments(List<String> arguments) {
    return arguments.stream().map(arg -> "'" + arg + "'").collect(Collectors.toList());
  }

  public static String unquoteArgument(String argument) {
    return argument.replaceAll("^'|'$", "");
  }
}
