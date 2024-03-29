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

import java.nio.file.Path;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.retryable.Retry;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;

public interface CmdRunner {

  @Flogger
  final class Logger {
    private Logger() {}
  }

  int EXIT_CODE_SUCCESS = 0;

  /**
   * Executes a command.
   *
   * @param cmd the command to execute
   * @return the execution result
   */
  StageExecutorResult execute(String cmd);

  /**
   * Writes string to a file.
   *
   * @param str the string
   * @param file the file path
   */
  void writeFile(String str, Path file);

  /**
   * Reads last lines of a file to a string using 'tail -n' command.
   *
   * @param file the file path
   * @param lastLines the number of last lines
   * @throws PipeliteException if the file could not be read
   */
  default String readFile(Path file, int lastLines) {
    try {
      StageExecutorResult result = Retry.DEFAULT.execute(this, "tail -n " + lastLines + " " + file);
      if (!result.isSuccess()) {
        throw new PipeliteException("Failed to read file: " + file);
      }
      return result.stdOut();
    } catch (PipeliteException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new PipeliteException("Failed to read file: " + file, ex);
    }
  }

  /**
   * Deletes a file using 'rm -f' command.
   *
   * @param file the file path
   * @return true if the file was deleted
   */
  default boolean deleteFile(Path file) {
    String filePath = file.toAbsolutePath().toString();
    try {
      execute("rm -f " + filePath);
    } catch (Exception ex) {
      Logger.log.atSevere().withCause(ex).log("Failed to delete file: " + filePath);
      return false;
    }
    return true;
  }

  /**
   * Checks if the file exists using 'test -f' command.
   *
   * @param file the file path
   * @return true if the file exists, false if the file does not exist or if the command failed
   */
  default boolean fileExists(Path file) {
    try {
      return execute("test -f " + file).isSuccess();
    } catch (Exception ex) {
      Logger.log.atSevere().withCause(ex).log("Failed to check if file exists: " + file);
      return false;
    }
  }

  /**
   * Checks if the directory exists using 'test -d' command.
   *
   * @param dir the directory path
   * @return true if the directory exists, false if the directory does not exist or if the command
   *     failed
   */
  default boolean dirExists(Path dir) {
    try {
      return execute("test -d " + dir).isSuccess();
    } catch (Exception ex) {
      Logger.log.atSevere().withCause(ex).log("Failed to check if directory exists: " + dir);
      return false;
    }
  }

  /**
   * Creates a temp file using the mktemp command.
   *
   * @return the temp file path or null if the file could not be created.
   * @throws PipeliteException if could not create the temp file
   */
  default String createTempFile() {
    try {
      StageExecutorResult result = execute("mktemp");
      if (!result.isSuccess() || result.stdOut() == null || result.stdOut().isEmpty()) {
        throw new PipeliteException("Failed to create temp file");
      } else {
        return result.stdOut().trim();
      }
    } catch (PipeliteException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new PipeliteException("Failed to create temp file", ex);
    }
  }

  /**
   * Returns execution result.
   *
   * @param cmd the command to execute
   * @param exitCode the exit code
   * @param stdout the stdout
   * @param stderr the stderr
   * @return the execution result
   */
  static StageExecutorResult result(String cmd, int exitCode, String stdout, String stderr) {
    StageExecutorResult result;
    result =
        (exitCode == EXIT_CODE_SUCCESS)
            ? StageExecutorResult.success()
            : StageExecutorResult.executionError();
    result.attribute(StageExecutorResultAttribute.COMMAND, cmd);
    result.attribute(StageExecutorResultAttribute.EXIT_CODE, exitCode);
    result.stdOut(stdout);
    result.stdErr(stderr);
    return result;
  }

  static CmdRunner create(CmdExecutorParameters executorParams) {
    if (executorParams.getHost() != null && !executorParams.getHost().trim().isEmpty()) {
      return new SshCmdRunner(executorParams);
    } else {
      return new LocalCmdRunner(executorParams);
    }
  }
}
