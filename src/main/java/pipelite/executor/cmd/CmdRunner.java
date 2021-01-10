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

import java.nio.file.Path;

import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;

public interface CmdRunner {

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
   * @param path the file
   * @throws java.io.IOException
   */
  void writeFile(String str, Path path);

  /**
   * Deletes a file.
   *
   * @param path the file path
   * @throws java.io.IOException
   */
  void deleteFile(Path path);

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
    StageExecutorResult result =
        (exitCode == EXIT_CODE_SUCCESS)
            ? StageExecutorResult.success()
            : StageExecutorResult.error();
    result.addAttribute(StageExecutorResultAttribute.COMMAND, cmd);
    result.addAttribute(StageExecutorResultAttribute.EXIT_CODE, exitCode);
    result.setStageLog((stdout != null ? stdout : "") + (stderr != null ? stderr : ""));
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
