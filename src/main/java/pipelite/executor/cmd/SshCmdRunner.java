/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor.cmd;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;

/** Executes a command over ssh. */
@Flogger
public class SshCmdRunner implements CmdRunner {

  @Value
  private static final class SshExecutorKey {
    private final String host;
    private final String user;
  }

  private static final int MAX_SSH_EXECUTORS =
      10; // Normally supported 10 maximum simultaneous ssh sessions.
  private static final ConcurrentHashMap<SshExecutorKey, Semaphore> sshExecutor =
      new ConcurrentHashMap<>();

  private final CmdExecutorParameters executorParams;

  public SshCmdRunner(CmdExecutorParameters executorParams) {
    this.executorParams = executorParams;
  }

  @Override
  public StageExecutorResult execute(String cmd) {
    if (cmd == null || cmd.trim().isEmpty()) {
      throw new PipeliteException("No command to execute");
    }

    String user = executorParams.resolveUser();
    String host = executorParams.getHost();

    SshExecutorKey key = new SshExecutorKey(host, user);

    sshExecutor.computeIfAbsent(key, k -> new Semaphore(MAX_SSH_EXECUTORS, true));

    // Limit the number of parallel ssh sessions.
    try {
      // Acquire a permit to execute an ssh command.
      sshExecutor.get(key).acquire();
      StageExecutorResult result =
          LocalCmdRunner.execute(
              "ssh",
              executorParams.getEnv(),
              "-o",
              "StrictHostKeyChecking=no",
              user + "@" + host,
              cmd);
      // Override the command attribute to preserve the original command in the stage execution
      // result.
      result.attribute(StageExecutorResultAttribute.COMMAND, cmd);
      return result;
    } catch (Exception ex) {
      throw new PipeliteException("Failed to execute command: " + cmd, ex);
    } finally {
      // Release the permit to execute an ssh command.
      sshExecutor.get(key).release();
    }
  }

  @Override
  public void writeFile(String str, Path path) {
    log.atInfo().log("Writing file " + path);

    String user = executorParams.resolveUser();
    String host = executorParams.getHost();
    String tempFileName;

    try {
      Path tempFile = Files.createTempFile(null, null);
      tempFileName = tempFile.toAbsolutePath().toString();

      String arg1 = tempFileName;
      String arg2 = user + "@" + host + ":" + path.toString();

      Files.write(tempFile, str.getBytes(StandardCharsets.UTF_8));

      StageExecutorResult result = LocalCmdRunner.execute("scp", arg1, arg2);
      if (!result.isSuccess()) {
        log.atSevere().log("Failed to write file " + path);
        throw new PipeliteException("Failed to write file " + path);
      }
    } catch (PipeliteException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new PipeliteException("Failed to write file " + path, ex);
    }
  }
}
