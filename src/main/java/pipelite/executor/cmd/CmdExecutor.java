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

import lombok.Data;
import lombok.extern.flogger.Flogger;
import pipelite.executor.StageExecutor;
import pipelite.executor.cmd.runner.*;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultExitCode;

@Data
@Flogger
public class CmdExecutor implements StageExecutor {

  /** The command string to be executed. */
  private String cmd;

  /** The type of runner that will be used to execute the command. */
  private CmdRunnerType cmdRunnerType;

  public String getDispatcherCmd(String pipelineName, String processId, Stage stage) {
    return null;
  }

  public void getDispatcherJobId(StageExecutionResult stageExecutionResult) {}

  public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
    String singularityImage = stage.getStageParameters().getSingularityImage();

    String execCmd = cmd;

    String dispatchCommand = getDispatcherCmd(pipelineName, processId, stage);
    if (dispatchCommand != null) {
      String cmdPrefix = dispatchCommand + " ";
      if (singularityImage != null) {
        cmdPrefix += "singularity run " + singularityImage + " ";
      }
      execCmd = cmdPrefix + execCmd;
    } else {
      if (singularityImage != null) {
        execCmd = "singularity run " + singularityImage + " " + execCmd;
      }
    }

    try {
      CmdRunner cmdRunner = getCmdRunner();

      CmdRunnerResult cmdRunnerResult = cmdRunner.execute(execCmd, stage.getStageParameters());

      StageExecutionResult result =
          StageExecutionResultExitCode.resolve(cmdRunnerResult.getExitCode());
      result.addAttribute(StageExecutionResult.COMMAND, execCmd);
      result.addAttribute(StageExecutionResult.HOST, stage.getStageParameters().getHost());
      result.addAttribute(StageExecutionResult.EXIT_CODE, cmdRunnerResult.getExitCode());
      result.setStdout(cmdRunnerResult.getStdout());
      result.setStderr(cmdRunnerResult.getStderr());
      return result;

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed call: %s", execCmd);
      StageExecutionResult result = StageExecutionResult.error();
      result.addAttribute(StageExecutionResult.COMMAND, execCmd);
      result.addExceptionAttribute(ex);
      getDispatcherJobId(result);
      return result;
    }
  }

  protected CmdRunner getCmdRunner() {
    CmdRunner cmdRunner;
    switch (cmdRunnerType) {
      case LOCAL_CMD_RUNNER:
        cmdRunner = new LocalCmdRunner();
        break;
      case SSH_CMD_RUNNER:
        cmdRunner = new SshCmdRunner();
        break;
      default:
        throw new RuntimeException("Unsupported command runner: " + cmdRunnerType.name());
    }
    return cmdRunner;
  }

  public String getWorkDir(Stage stage) {
    if (stage.getStageParameters().getWorkDir() != null) {
      return stage.getStageParameters().getWorkDir();
    } else {
      return "";
    }
  }

  public String getWorkFile(
      String pipelineName, String processId, Stage stage, String prefix, String suffix) {
    String workDir = getWorkDir(stage);
    if (!workDir.isEmpty() && !workDir.endsWith("/")) {
      workDir += "/";
    }
    return workDir
        + "pipelite-"
        + prefix
        + "-"
        + pipelineName
        + "_"
        + processId
        + "_"
        + stage.getStageName()
        + "."
        + suffix;
  }
}
