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
package pipelite.executor;

import pipelite.executor.cmd.CmdExecutor;
import pipelite.executor.cmd.LsfCmdExecutor;
import pipelite.executor.cmd.runner.CmdRunnerType;
import pipelite.json.Json;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;

public interface StageExecutor {

  /**
   * Called repeatedly to execute the stage until it is not ACTIVE. Only asynchronous executions are
   * expected to return ACTIVE.
   *
   *
   * @param pipelineName the pipeline name.
   * @param processId the process id.
   * @param stage the stage to be executed.
   * @return stage execution result.
   */
  StageExecutionResult execute(String pipelineName, String processId, Stage stage);

  default String serialize() {
    return Json.serialize(this);
  }

  static StageExecutor deserialize(String className, String json) {
    return Json.deserialize(json, className, StageExecutor.class);
  }

  static CmdExecutor createCmdExecutor(String cmd, CmdRunnerType cmdRunnerType) {
    CmdExecutor cmdExecutor = new CmdExecutor();
    cmdExecutor.setCmd(cmd);
    cmdExecutor.setCmdRunnerType(cmdRunnerType);
    return cmdExecutor;
  }

  static CmdExecutor createLocalCmdExecutor(String cmd) {
    return createCmdExecutor(cmd, CmdRunnerType.LOCAL_CMD_RUNNER);
  }

  static CmdExecutor createSshCmdExecutor(String cmd) {
    return createCmdExecutor(cmd, CmdRunnerType.SSH_CMD_RUNNER);
  }

  static LsfCmdExecutor createLsfCmdExecutor(String cmd, CmdRunnerType cmdRunnerType) {
    LsfCmdExecutor lsfCmdExecutor = new LsfCmdExecutor();
    lsfCmdExecutor.setCmd(cmd);
    lsfCmdExecutor.setCmdRunnerType(cmdRunnerType);
    return lsfCmdExecutor;
  }

  static LsfCmdExecutor createLsfLocalCmdExecutor(String cmd) {
    return createLsfCmdExecutor(cmd, CmdRunnerType.LOCAL_CMD_RUNNER);
  }

  static LsfCmdExecutor createLsfSshCmdExecutor(String cmd) {
    return createLsfCmdExecutor(cmd, CmdRunnerType.SSH_CMD_RUNNER);
  }
}
