package pipelite.executor.executable;

import lombok.extern.flogger.Flogger;
import org.apache.commons.exec.*;
import pipelite.executor.output.KeepOldestByteArrayOutputStream;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

@Flogger
public abstract class AbstractSystemCallExecutor implements ExecutableTaskExecutor {

  private long timeout;

  public abstract String getExecutable();

  public abstract List<String> getArguments(TaskInstance taskInstance);

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {

    String commandLineString = null;

    try {
      CommandLine commandLine = new CommandLine(getExecutable());
      commandLine.addArguments(getArguments(taskInstance).toArray(new String[0]));
      commandLineString = getExecutable() + " " + String.join(" ", getArguments(taskInstance));

      OutputStream stdoutStream = new KeepOldestByteArrayOutputStream();
      OutputStream stderrStream = new KeepOldestByteArrayOutputStream();

      Executor executor = new DefaultExecutor();

      executor.setExitValues(null);

      executor.setStreamHandler(new PumpStreamHandler(stdoutStream, stderrStream));
      // executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));

      executor.setWatchdog(
          new ExecuteWatchdog(timeout > 0 ? timeout : ExecuteWatchdog.INFINITE_TIMEOUT));

      log.atInfo().log("Executing system call: %s" + commandLineString);

      int exitCode = executor.execute(commandLine, taskInstance.getTaskParameters().getEnvAsMap());

      TaskExecutionResult result = resolve(taskInstance, exitCode);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, commandLineString);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, getHost());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE, exitCode);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT, getStream(stdoutStream));
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR, getStream(stderrStream));
      return result;

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed system call: %s", commandLineString);
      TaskExecutionResult result = TaskExecutionResult.defaultInternalError();
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, commandLineString);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, getHost());
      result.addExceptionAttribute(ex);
      return result;
    }
  }

  private String getStream(OutputStream stdoutStream) {
    try {
      stdoutStream.flush();
      String value = stdoutStream.toString();
      log.atInfo().log(value);
      stdoutStream.close();
      return value;
    } catch (IOException e) {
      return null;
    } finally {
    }
  }

  private String getHost() {
    return null == System.getenv("HOSTNAME")
        ? System.getenv("COMPUTERNAME")
        : System.getenv("HOSTNAME");
  }
}
