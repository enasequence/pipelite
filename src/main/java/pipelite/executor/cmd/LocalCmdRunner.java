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

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
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

  /**
   * Executes a command using the /bin/sh shell with a five minutes timeout. The command arguments
   * are interpreted by the /bin/sh shell.
   *
   * @param cmd the command to execute
   * @return the stage execution result
   */
  @Override
  public StageExecutorResult execute(String cmd) {
    // Use /bin/sh -c to delegate argument parsing to the shell. The command
    // and the arguments are treated as a single argument passed to the
    // shell. The shell will then parse the command and the arguments. Argument
    // parsing without using the shell is very challenging because of single and
    // double quotes and other complications.
    return execute("/bin/sh", executorParams.getEnv(), "-c", cmd);
  }

  /**
   * Executes a command with a five minutes timeout. The command and the command arguments must be
   * provided separately.
   *
   * @return the stage execution result
   */
  public static StageExecutorResult execute(String cmd, String... args) {
    return execute(cmd, null, args);
  }

  /**
   * Executes a command with a five minutes timeout. The command and the command arguments must be
   * provided separately.
   *
   * @param cmd the command to execute
   * @param env the environmental variables
   * @return the stage execution result
   */
  public static StageExecutorResult execute(String cmd, Map<String, String> env, String... args) {
    if (cmd == null || cmd.isEmpty()) {
      throw new PipeliteException("No command to execute");
    }

    Duration timeout = Duration.ofMinutes(5);

    String fullCmd = cmd;
    CommandLine commandLine = new CommandLine(cmd);
    for (String arg : args) {
      fullCmd += " " + arg;
      commandLine.addArgument(arg, false);
    }

    try {
      ZonedDateTime startTime = ZonedDateTime.now();

      OutputStream stdoutStream = new ByteArrayOutputStream();
      OutputStream stderrStream = new ByteArrayOutputStream();

      Executor apacheExecutor = new DefaultExecutor();
      apacheExecutor.setExitValues(null);
      apacheExecutor.setStreamHandler(new PumpStreamHandler(stdoutStream, stderrStream));

      if (timeout != null) {
        apacheExecutor.setWatchdog(new ExecuteWatchdog(timeout.toMillis()));
      }

      log.atInfo().log("Executing command: " + fullCmd);

      int exitCode =
          (env != null)
              ? apacheExecutor.execute(commandLine, env)
              : apacheExecutor.execute(commandLine);

      Duration callDuration = Duration.between(startTime, ZonedDateTime.now());

      String stdOut = getStringFromStream(stdoutStream);
      String stdErr = getStringFromStream(stderrStream);

      log.atInfo().log(
          "Finished executing command in "
              + callDuration.toSeconds()
              + " seconds with exit code "
              + exitCode
              + ": "
              + fullCmd);
      if (stdOut != null && !stdOut.isEmpty()) {
        log.atFine().log("stdout: " + stdOut);
      }
      if (stdErr != null && !stdErr.isEmpty()) {
        log.atFine().log("stderr: " + stdErr);
      }

      return CmdRunner.result(fullCmd, exitCode, stdOut, stdErr);
    } catch (Exception ex) {
      throw new PipeliteException("Failed to execute command: " + fullCmd, ex);
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

  private static String getStringFromStream(OutputStream stdoutStream) {
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
