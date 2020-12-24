/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor.cmd.runner;

import static pipelite.stage.StageExecutionResultExitCode.EXIT_CODE_ERROR;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.flogger.Flogger;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.session.SessionHeartbeatController;
import pipelite.executor.StageExecutorParameters;
import pipelite.executor.stream.KeepOldestByteArrayOutputStream;
/** Executes a command over ssh. */
@Flogger
public class SshCmdRunner implements CmdRunner {

  public static final int SSH_PORT = 22;
  public static final int SSH_VERIFY_SECONDS = 60;
  public static final int SSH_HEARTBEAT_SECONDS = 10;

  @Override
  public CmdRunnerResult execute(String cmd, StageExecutorParameters executorParams) {
    if (cmd == null) {
      return new CmdRunnerResult(
          EXIT_CODE_ERROR, null, null, CmdRunnerResult.InternalError.NULL_CMD);
    }
    if (cmd.trim().isEmpty()) {
      return new CmdRunnerResult(
          EXIT_CODE_ERROR, null, null, CmdRunnerResult.InternalError.EMPTY_CMD);
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
              .connect(user, executorParams.getHost(), SSH_PORT)
              .verify(SSH_VERIFY_SECONDS, TimeUnit.SECONDS)
              .getSession()) {

        session.auth().verify(SSH_VERIFY_SECONDS, TimeUnit.SECONDS);

        session.setSessionHeartbeat(
            SessionHeartbeatController.HeartbeatType.IGNORE,
            Duration.ofSeconds(SSH_HEARTBEAT_SECONDS));

        ClientChannel channel = session.createExecChannel(cmd, null, executorParams.getEnv());
        channel.setOut(stdoutStream);
        channel.setErr(stderrStream);

        channel.open().verify(SSH_VERIFY_SECONDS, TimeUnit.SECONDS);

        Set<ClientChannelEvent> events =
            channel.waitFor(EnumSet.of(ClientChannelEvent.CLOSED), executorParams.getTimeout());

        if (events.contains(ClientChannelEvent.TIMEOUT)) {
          log.atSevere().log("Failed ssh call because of timeout: %s", cmd);
          return new CmdRunnerResult(
              EXIT_CODE_ERROR, null, null, CmdRunnerResult.InternalError.TIMEOUT_CMD);
        }

        int exitCode = channel.getExitStatus();
        return new CmdRunnerResult(
            exitCode, getStream(stdoutStream), getStream(stderrStream), null);
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed ssh call: %s", cmd);
      return new CmdRunnerResult(
          EXIT_CODE_ERROR, null, null, CmdRunnerResult.InternalError.EXCEPTION_CMD);
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
      stdoutStream.close();
    } catch (IOException e) {
    } finally {
      return value;
    }
  }
}
