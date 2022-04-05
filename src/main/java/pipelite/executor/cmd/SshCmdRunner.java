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
package pipelite.executor.cmd;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.flogger.Flogger;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.keyverifier.AcceptAllServerKeyVerifier;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.client.subsystem.sftp.SftpClient;
import org.apache.sshd.client.subsystem.sftp.impl.DefaultSftpClientFactory;
import org.apache.sshd.common.session.SessionHeartbeatController;
import pipelite.exception.PipeliteException;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.CmdExecutorParameters;

/** Executes a command over ssh. */
@Flogger
public class SshCmdRunner implements CmdRunner {

  public static final int SSH_PORT = 22;
  public static final int SSH_VERIFY_TIMEOUT = 120;
  public static final int SSH_HEARTBEAT_SECONDS = 10;

  private static final SshClient sshClient;

  private final CmdExecutorParameters executorParams;

  public SshCmdRunner(CmdExecutorParameters executorParams) {
    this.executorParams = executorParams;
  }

  static {
    sshClient = SshClient.setUpDefaultClient();
    sshClient.setServerKeyVerifier(AcceptAllServerKeyVerifier.INSTANCE);
    sshClient.start();
  }

  @Override
  public StageExecutorResult execute(String cmd) {
    if (cmd == null || cmd.trim().isEmpty()) {
      throw new PipeliteException("No command to execute");
    }

    log.atInfo().log("Executing ssh call: %s", cmd);

    ZonedDateTime startTIme = ZonedDateTime.now();

    try (ClientSession session = createSession(executorParams);
        ClientChannel channel = session.createExecChannel(cmd, null, executorParams.getEnv())) {

      OutputStream stdoutStream = new ByteArrayOutputStream();
      OutputStream stderrStream = new ByteArrayOutputStream();
      channel.setOut(stdoutStream);
      channel.setErr(stderrStream);

      channel.open().verify(SSH_VERIFY_TIMEOUT, TimeUnit.SECONDS);

      Set<ClientChannelEvent> events =
          channel.waitFor(
              EnumSet.of(
                  ClientChannelEvent.EXIT_STATUS,
                  ClientChannelEvent.EXIT_SIGNAL,
                  ClientChannelEvent.CLOSED),
              executorParams.getTimeout());
      if (events.contains(ClientChannelEvent.TIMEOUT)) {
        throw new PipeliteException("Failed to execute ssh call because of timeout: " + cmd);
      }

      Integer exitCode = channel.getExitStatus();

      if (exitCode == null) {
        String exitSignal = channel.getExitSignal();
        String exitSignalStr = "";
        if (exitSignal != null) {
          exitSignalStr = " (exit signal " + exitSignal + ")";
        }
        throw new PipeliteException(
            "Failed to execute ssh call because of missing exit code" + exitSignalStr + ": " + cmd);
      }

      Duration callDuration = Duration.between(startTIme, ZonedDateTime.now());

      log.atInfo().log(
          "Finished executing ssh call (exit code "
              + exitCode
              + ") in "
              + callDuration.toSeconds()
              + " seconds: %s",
          cmd);
      return CmdRunner.result(cmd, exitCode, getStream(stdoutStream), getStream(stderrStream));

    } catch (PipeliteException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new PipeliteException("Failed to execute ssh call: " + cmd, ex);
    }
  }

  @Override
  public void writeFile(String str, Path path) {
    log.atInfo().log("Writing file %s", path);
    try (ClientSession session = createSession(executorParams);
        SftpClient sftpClient = DefaultSftpClientFactory.INSTANCE.createSftpClient(session)) {
      CmdRunnerUtils.write(
          str,
          sftpClient.write(
              path.toString(),
              SftpClient.OpenMode.Write,
              SftpClient.OpenMode.Create,
              SftpClient.OpenMode.Truncate));
    } catch (IOException ex) {
      throw new PipeliteException("Failed to write file " + path, ex);
    }
  }

  private ClientSession createSession(CmdExecutorParameters executorParams) throws IOException {
    String user = executorParams.resolveUser();
    ClientSession session =
        sshClient
            .connect(user, executorParams.getHost(), SSH_PORT)
            .verify(SSH_VERIFY_TIMEOUT, TimeUnit.SECONDS)
            .getSession();
    session.auth().verify(SSH_VERIFY_TIMEOUT, TimeUnit.SECONDS);
    session.setSessionHeartbeat(
        SessionHeartbeatController.HeartbeatType.IGNORE, Duration.ofSeconds(SSH_HEARTBEAT_SECONDS));
    return session;
  }

  private String getStream(OutputStream stdoutStream) {
    String value = null;
    try {
      stdoutStream.flush();
      value = stdoutStream.toString();
      stdoutStream.close();
    } catch (IOException ignored) {
    }
    return value;
  }
}
