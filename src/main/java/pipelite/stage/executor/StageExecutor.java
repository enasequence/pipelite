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
package pipelite.stage.executor;

import pipelite.executor.CmdExecutor;
import pipelite.executor.LsfCmdExecutor;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.cmd.LocalCmdRunner;
import pipelite.executor.cmd.SshCmdRunner;
import pipelite.json.Json;
import pipelite.stage.Stage;

/** Executes a stage. Must be serializable to json. */
public interface StageExecutor {

  /**
   * Called repeatedly to execute the stage until it is not ACTIVE. Only asynchronous executions are
   * expected to return ACTIVE.
   *
   * @param pipelineName the pipeline name.
   * @param processId the process id.
   * @param stage the stage to be executed.
   * @return stage execution result
   */
  StageExecutorResult execute(String pipelineName, String processId, Stage stage);

  /** Serializes the executor to json. */
  default String serialize() {
    return Json.serialize(this);
  }

  /** Deserializes the executor from json. */
  static StageExecutor deserialize(String className, String json) {
    return Json.deserialize(json, className, StageExecutor.class);
  }

  /**
   * Creates a command executor.
   *
   * @param cmd the command
   * @param cmdRunner the command runner
   * @return a command executor.
   */
  static CmdExecutor createCmdExecutor(String cmd, CmdRunner cmdRunner) {
    CmdExecutor cmdExecutor = new CmdExecutor();
    cmdExecutor.setCmd(cmd);
    cmdExecutor.setCmdRunner(cmdRunner);
    return cmdExecutor;
  }

  /**
   * Creates an executor that executes the command on the local host.
   *
   * @param cmd the command
   * @return an executor that executes the command on the local host.
   */
  static CmdExecutor createLocalCmdExecutor(String cmd) {
    return createCmdExecutor(cmd, new LocalCmdRunner());
  }

  /**
   * Creates an executor that connects to a remote host using ssh and executes the command.
   *
   * @param cmd the command
   * @return an executor that connects to a remote host using ssh and executes the command.
   */
  static CmdExecutor createSshCmdExecutor(String cmd) {
    return createCmdExecutor(cmd, new SshCmdRunner());
  }

  /**
   * Creates an executor that executes the command using LSF.
   *
   * @param cmd the command
   * @param cmdRunner the command runner
   * @return an executor that executes the command using LSF.
   */
  static LsfCmdExecutor createLsfCmdExecutor(String cmd, CmdRunner cmdRunner) {
    LsfCmdExecutor lsfCmdExecutor = new LsfCmdExecutor();
    lsfCmdExecutor.setCmd(cmd);
    lsfCmdExecutor.setCmdRunner(cmdRunner);
    return lsfCmdExecutor;
  }

  /**
   * Creates an executor that executes the command using LSF on the local host.
   *
   * @param cmd the command
   * @return an executor that executes the command using LSF on the local host.
   */
  static LsfCmdExecutor createLsfLocalCmdExecutor(String cmd) {
    return createLsfCmdExecutor(cmd, new LocalCmdRunner());
  }

  /**
   * Creates an executor that connects to a remote host using ssh and executes the command using
   * LSF.
   *
   * @param cmd the command
   * @return an executor that connects to a remote host using ssh and executes the command using
   *     LSF.
   */
  static LsfCmdExecutor createLsfSshCmdExecutor(String cmd) {
    return createLsfCmdExecutor(cmd, new SshCmdRunner());
  }
}
