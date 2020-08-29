package pipelite.executor;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import org.apache.commons.exec.*;
import org.apache.commons.exec.util.StringUtils;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.session.SessionHeartbeatController;
import pipelite.executor.output.KeepOldestLimitedByteArrayOutputStream;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Flogger
@Value
@Builder
public class SshCallTaskExecutor implements TaskExecutor {

  public static final int MAX_STDOUT_BYTES = 1024 * 1024;
  public static final int MAX_STDERR_BYTES = 1024 * 1024;

  public static final int SSH_PORT = 22;
  public static final int SSH_TIMEOUT_SECONDS = 60;
  public static final int SSH_HEARTBEAT_SECONDS = 10;

  @NonNull private final String executable;
  @NonNull private final Collection<String> arguments;
  private Resolver resolver;
  private long timeout;

  public interface Resolver {
    TaskExecutionResult resolve(TaskInstance taskInstance, int exitCode);
  }

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {

    String user = System.getProperty("user.name");
    String host = taskInstance.getTaskParameters().getHost();
    Map<String, String> env = taskInstance.getTaskParameters().getEnvAsMap();
    String cmd = getCommand();

    log.atInfo().log("Executing ssh call: %s", cmd);

    OutputStream stdoutStream = new KeepOldestLimitedByteArrayOutputStream();
    OutputStream stderrStream = new KeepOldestLimitedByteArrayOutputStream();

    SshClient client = SshClient.setUpDefaultClient();

    client.start();

    try (ClientSession session =
        client
            .connect(user, host, SSH_PORT)
            .verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .getSession()) {

      session.auth().verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      session.setSessionHeartbeat(
          SessionHeartbeatController.HeartbeatType.IGNORE,
          Duration.ofSeconds(SSH_HEARTBEAT_SECONDS));

      ClientChannel channel = session.createExecChannel(cmd, null, env);
      channel.setOut(stdoutStream);
      channel.setErr(stderrStream);

      channel.open().verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      channel.waitFor(EnumSet.of(ClientChannelEvent.CLOSED), 0L);

      int exitCode = channel.getExitStatus();

      TaskExecutionResult result;
      if (resolver != null) {
        result = resolver.resolve(taskInstance, exitCode);
      } else {
        result = taskInstance.getResolver().resolve(exitCode);
      }
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, cmd);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, host);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE, exitCode);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT, getStream(stdoutStream));
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR, getStream(stderrStream));

      return result;

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Ssh call execution failed: %s", getCommand());
      return TaskExecutionResult.defaultPermanentError();
    } finally {
      if (client != null) {
        client.stop();
      }
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

  private String getCommand() {
    return StringUtils.toString(createCommandLine().toStrings(), " ");
  }

  public String toString() {
    return getCommand();
  }
}
