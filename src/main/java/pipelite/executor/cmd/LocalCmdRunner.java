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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import lombok.extern.flogger.Flogger;
import org.apache.commons.exec.*;
import pipelite.exception.PipeliteException;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.CmdExecutorParameters;

@Flogger
public class LocalCmdRunner implements CmdRunner {

  private final CmdExecutorParameters executorParams;

  public LocalCmdRunner(CmdExecutorParameters executorParams) {
    this.executorParams = executorParams;
  }

  @Override
  public StageExecutorResult execute(String cmd) {
    if (cmd == null || cmd.isEmpty()) {
      throw new PipeliteException("No command to execute");
    }

    try {
      CommandLine commandLine = new CommandLine("/bin/sh");
      commandLine.addArgument("-c");
      commandLine.addArgument(cmd, false);

      OutputStream stdoutStream = new ByteArrayOutputStream();
      OutputStream stderrStream = new ByteArrayOutputStream();

      Executor apacheExecutor = new DefaultExecutor();

      apacheExecutor.setExitValues(null);

      apacheExecutor.setStreamHandler(new PumpStreamHandler(stdoutStream, stderrStream));
      // executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));

      Duration timeout = executorParams.getTimeout();
      if (timeout != null) {
        apacheExecutor.setWatchdog(
            new ExecuteWatchdog(
                executorParams.getTimeout() != null
                    ? executorParams.getTimeout().toMillis()
                    : ExecuteWatchdog.INFINITE_TIMEOUT));
      }

      log.atInfo().log("Executing local call: %s", cmd);

      int exitCode = apacheExecutor.execute(commandLine, executorParams.getEnv());
      return CmdRunner.result(cmd, exitCode, getStream(stdoutStream), getStream(stderrStream));

    } catch (Exception ex) {
      throw new PipeliteException("Failed to execute local call: " + cmd, ex);
    }
  }

  @Override
  public void writeFile(String str, Path path) {
    log.atInfo().log("Writing file %s", path);
    try {
      CmdRunnerUtils.write(str, new FileOutputStream(path.toFile()));
    } catch (IOException ex) {
      throw new PipeliteException("Failed to write file " + path, ex);
    }
  }

  private String getStream(OutputStream stdoutStream) {
    try {
      stdoutStream.flush();
      String value = stdoutStream.toString();
      // log.atInfo().log(value);
      stdoutStream.close();
      return value;
    } catch (IOException e) {
      return null;
    }
  }
}
