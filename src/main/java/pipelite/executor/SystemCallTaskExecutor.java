package pipelite.executor;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
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
@Value
@Builder
public class SystemCallTaskExecutor implements TaskExecutor {

  public static final int MAX_STDOUT_BYTES = 1024 * 1024;
  public static final int MAX_STDERR_BYTES = 1024 * 1024;

  @NonNull private final String executable;
  @NonNull private final Collection<String> arguments;
  private Resolver resolver;
  private long timeout;

  public interface Resolver {
    TaskExecutionResult resolve(TaskInstance taskInstance, int exitCode);
  }

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

      log.atInfo().log("Executing system call: %s", getCommand());

      int exitCode = executor.execute(commandLine, taskInstance.getTaskParameters().getEnvAsMap());

      TaskExecutionResult result;
      if (resolver != null) {
        result = resolver.resolve(taskInstance, exitCode);
      } else {
        result = taskInstance.getResolver().resolve(exitCode);
      }
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
      log.atSevere().withCause(ex).log("System call execution failed: %s", getCommand());

      TaskExecutionResult result = TaskExecutionResult.defaultInternalError();
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
      log.atInfo().log(value);
      stdoutStream.close();
    } catch (IOException e) {
    } finally {
      return value;
    }
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
