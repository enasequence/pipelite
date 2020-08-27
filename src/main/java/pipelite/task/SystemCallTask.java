package pipelite.task;

import lombok.extern.flogger.Flogger;
import org.apache.commons.exec.*;
import org.apache.commons.exec.util.StringUtils;
import pipelite.instance.TaskInstance;
import pipelite.task.result.TaskExecutionResult;

import java.io.*;
import java.util.Collection;

@Flogger
public abstract class SystemCallTask implements Task {
  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {

    Executor executor = new DefaultExecutor();

    try {
      executor.setExitValues(null);

      executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));

      executor.setWatchdog(
          new ExecuteWatchdog(timeout > 0 ? timeout : ExecuteWatchdog.INFINITE_TIMEOUT));

      log.atInfo().log(toString());

      CommandLine commandLine = createCommandLine();

      int exitCode = executor.execute(commandLine, taskInstance.getTaskParameters().getEnvAsMap());

      TaskExecutionResult result = taskInstance.getTaskParameters().getResolver().resolve(exitCode);

      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, getCommand());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, getHost());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE, exitCode);

      // TODO: consider supporting
      // TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR
      // TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT

      if (executor.getWatchdog() != null && executor.getWatchdog().killedProcess()) {
        result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_TIMEOUT, timeout);
      }

      return result;

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Exception during system call");

      TaskExecutionResult result = TaskExecutionResult.internalError();
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, getCommand());
      result.addExceptionAttribute(ex);

      return result;
    }
  }

  protected abstract String getExecutable();

  protected abstract Collection<String> getArguments();

  private long timeout;

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  private CommandLine createCommandLine() {
    CommandLine commandLine = new CommandLine(getExecutable());
    if (null != getArguments()) {
      for (String arg : getArguments()) commandLine.addArgument(arg, false);
    }
    return commandLine;
  }

  private String getHost() {
    return null == System.getenv("HOSTNAME")
        ? System.getenv("COMPUTERNAME")
        : System.getenv("HOSTNAME");
  }

  private String getCommand() {
    return StringUtils.toString(createCommandLine().toStrings(), " ");
  }

  public String toString() {
    return getCommand();
  }
}
