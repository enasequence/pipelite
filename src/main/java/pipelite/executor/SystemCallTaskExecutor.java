package pipelite.executor;

import lombok.extern.flogger.Flogger;
import org.apache.commons.exec.*;
import org.apache.commons.exec.util.StringUtils;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.base.external.ConstraintedOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

@Flogger
public abstract class SystemCallTaskExecutor implements TaskExecutor {

  public static final int MAX_STDOUT_BYTES = 1024 * 1024;
  public static final int MAX_STDERR_BYTES = 1024 * 1024;

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {

    OutputStream stdoutStream = new ConstrainedByteArrayOutputStream(MAX_STDOUT_BYTES);
    OutputStream stderrStream = new ConstraintedOutputStream(MAX_STDERR_BYTES);

    Executor executor = new DefaultExecutor();

    try {
      executor.setExitValues(null);

      executor.setStreamHandler(new PumpStreamHandler(stdoutStream, stderrStream));
      // executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));

      executor.setWatchdog(
          new ExecuteWatchdog(timeout > 0 ? timeout : ExecuteWatchdog.INFINITE_TIMEOUT));

      log.atInfo().log(toString());

      CommandLine commandLine = createCommandLine();

      int exitCode = executor.execute(commandLine, taskInstance.getTaskParameters().getEnvAsMap());

      TaskExecutionResult result = taskInstance.getResolver().resolve(exitCode);

      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, getCommand());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, getHost());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE, exitCode);

      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT, getStream(stdoutStream));
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR, getStream(stderrStream));

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

  private String getStream(OutputStream stdoutStream) {
    String value = null;
    try {
      stdoutStream.flush();
      value = stdoutStream.toString();
      stdoutStream.close();
    } catch (IOException e) {
    } finally {
      return value;
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
