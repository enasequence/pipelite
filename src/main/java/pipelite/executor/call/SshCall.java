package pipelite.executor.call;

import lombok.extern.flogger.Flogger;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.session.SessionHeartbeatController;
import pipelite.executor.stream.KeepOldestByteArrayOutputStream;
import pipelite.task.TaskParameters;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static pipelite.task.TaskExecutionResultExitCodeSerializer.EXIT_CODE_DEFAULT_INTERNAL_ERROR;
import static pipelite.task.TaskExecutionResultExitCodeSerializer.EXIT_CODE_DEFAULT_PERMANENT_ERROR;

@Flogger
public class SshCall implements CallExecutor.Call {

  public static final int SSH_PORT = 22;
  public static final int SSH_TIMEOUT_SECONDS = 60;
  public static final int SSH_HEARTBEAT_SECONDS = 10;

  @Override
  public CallExecutor.CallResult call(String cmd, TaskParameters taskParameters) {
    if (cmd == null) {
      return new CallExecutor.CallResult(EXIT_CODE_DEFAULT_PERMANENT_ERROR, null, null);
    }

    SshClient client = null;

    try {
      String user = System.getProperty("user.name");

      log.atInfo().log("Executing ssh call: %s", cmd);

      OutputStream stdoutStream = new KeepOldestByteArrayOutputStream();
      OutputStream stderrStream = new KeepOldestByteArrayOutputStream();

      client = SshClient.setUpDefaultClient();

      client.start();

      try (ClientSession session =
          client
              .connect(user, taskParameters.getHost(), SSH_PORT)
              .verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
              .getSession()) {

        session.auth().verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        session.setSessionHeartbeat(
            SessionHeartbeatController.HeartbeatType.IGNORE,
            Duration.ofSeconds(SSH_HEARTBEAT_SECONDS));

        ClientChannel channel = session.createExecChannel(cmd, null, taskParameters.getEnvAsMap());
        channel.setOut(stdoutStream);
        channel.setErr(stderrStream);

        channel.open().verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        channel.waitFor(EnumSet.of(ClientChannelEvent.CLOSED), taskParameters.getTimeout());

        int exitCode = channel.getExitStatus();
        return new CallExecutor.CallResult(exitCode, getStream(stdoutStream), getStream(stderrStream));
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed ssh call: %s", cmd);
      return new CallExecutor.CallResult(EXIT_CODE_DEFAULT_INTERNAL_ERROR, null, null);
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
