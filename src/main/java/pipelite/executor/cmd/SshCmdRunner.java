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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.CmdExecutorParameters;

/** Executes a command over ssh. */
@Flogger
public class SshCmdRunner implements CmdRunner {

  public static final int SSH_PORT = 22;
  public static final int SSH_VERIFY_TIMEOUT = 120;
  public static final int SSH_HEARTBEAT_SECONDS = 10;

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
    String localCmd = "ssh " + user + "@" + host + " " + cmd;

    try {
      LocalCmdRunner localCmdRunner = new LocalCmdRunner(executorParams);
      StageExecutorResult result = localCmdRunner.execute(localCmd);
      return result;
    } catch (PipeliteException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new PipeliteException("Failed to execute command: " + localCmd, ex);
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

      String localCmd =
          "scp "
              + tempFileName
              + " "
              + user
              + "@"
              + host
              + ":/"
              + path.toString().replaceAll("^\\/+", "");

      Files.write(tempFile, str.getBytes(StandardCharsets.UTF_8));

      LocalCmdRunner localCmdRunner = new LocalCmdRunner(executorParams);
      StageExecutorResult result = localCmdRunner.execute(localCmd);
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
