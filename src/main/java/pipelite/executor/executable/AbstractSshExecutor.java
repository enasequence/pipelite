package pipelite.executor.executable;

import lombok.extern.flogger.Flogger;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.session.SessionHeartbeatController;
import pipelite.executor.output.KeepOldestByteArrayOutputStream;
import pipelite.task.TaskInstance;
import pipelite.task.TaskExecutionResult;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.EnumSet;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Flogger
public abstract class AbstractSshExecutor implements ExecutableTaskExecutor {

  public static final int SSH_PORT = 22;
  public static final int SSH_TIMEOUT_SECONDS = 60;
  public static final int SSH_HEARTBEAT_SECONDS = 10;

  public abstract String getCommand(TaskInstance taskInstance);

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {

    String command = null;
    String host = null;
    SshClient client = null;

    try {
      command = getCommand(taskInstance);
      host = taskInstance.getTaskParameters().getHost();
      String user = System.getProperty("user.name");
      Map<String, String> env = taskInstance.getTaskParameters().getEnvAsMap();

      log.atInfo().log("Executing ssh call: %s", command);

      OutputStream stdoutStream = new KeepOldestByteArrayOutputStream();
      OutputStream stderrStream = new KeepOldestByteArrayOutputStream();

      client = SshClient.setUpDefaultClient();

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

        ClientChannel channel = session.createExecChannel(command, null, env);
        channel.setOut(stdoutStream);
        channel.setErr(stderrStream);

        channel.open().verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        channel.waitFor(EnumSet.of(ClientChannelEvent.CLOSED), 0L);

        int exitCode = channel.getExitStatus();

        TaskExecutionResult result = resolve(taskInstance, exitCode);
        result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, command);
        result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, host);
        result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE, exitCode);
        result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT, getStream(stdoutStream));
        result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR, getStream(stderrStream));
        return result;
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed ssh call: %s", command);
      TaskExecutionResult result = TaskExecutionResult.defaultInternalError();
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, command);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, host);
      result.addExceptionAttribute(ex);
      return result;
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
}
