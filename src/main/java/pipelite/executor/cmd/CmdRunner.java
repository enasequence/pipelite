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

import pipelite.stage.parameters.CmdExecutorParameters;

import java.io.IOException;
import java.nio.file.Path;

public interface CmdRunner {

  int EXIT_CODE_SUCCESS = 0;
  int EXIT_CODE_ERROR = 1;

  /**
   * Executes a command.
   *
   * @param cmd the command to execute
   * @param executorParams the executor parameters
   * @return the command execution result
   */
  CmdRunnerResult execute(String cmd, CmdExecutorParameters executorParams);

  /**
   * Writes string to a file.
   *
   * @param str the string
   * @param path the file
   * @param executorParams the executor parameters
   * @throws java.io.IOException
   */
  void writeFile(String str, Path path, CmdExecutorParameters executorParams) throws IOException;

  /**
   * Deletes a file.
   *
   * @param path the file path
   * @param executorParams the executor parameters
   * @throws java.io.IOException
   */
  void deleteFile(Path path, CmdExecutorParameters executorParams) throws IOException;
}
