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

import lombok.extern.flogger.Flogger;
import pipelite.executor.StageExecutor;
import pipelite.executor.cmd.runner.CmdRunner;
import pipelite.executor.cmd.runner.CmdRunnerResult;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultExitCode;

@Flogger
public abstract class CmdExecutor implements StageExecutor {

  public abstract CmdRunner getCmdRunner();

  public abstract String getCmd(Stage stage);

  public String getDispatcherCmd(Stage stage) {
    return null;
  }

  public void getDispatcherJobId(StageExecutionResult stageExecutionResult) {}

  public StageExecutionResult execute(Stage stage) {
    String cmd = getCmd(stage);

    String singularityImage = stage.getStageParameters().getSingularityImage();

    String dispatchCommand = getDispatcherCmd(stage);
    if (dispatchCommand != null) {
      String cmdPrefix = dispatchCommand + " ";
      if (singularityImage != null) {
        cmdPrefix += "singularity run " + singularityImage + " ";
      }
      cmd = cmdPrefix + cmd;
    } else {
      if (singularityImage != null) {
        cmd = "singularity run " + singularityImage + " " + cmd;
      }
    }

    try {
      CmdRunnerResult cmdRunnerResult = getCmdRunner().execute(cmd, stage.getStageParameters());

      StageExecutionResult result =
          StageExecutionResultExitCode.resolve(cmdRunnerResult.getExitCode());
      result.addAttribute(StageExecutionResult.COMMAND, cmd);
      result.addAttribute(StageExecutionResult.HOST, stage.getStageParameters().getHost());
      result.addAttribute(StageExecutionResult.EXIT_CODE, cmdRunnerResult.getExitCode());
      result.setStdout(cmdRunnerResult.getStdout());
      result.setStderr(cmdRunnerResult.getStderr());
      return result;

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed call: %s", cmd);
      StageExecutionResult result = StageExecutionResult.error();
      result.addAttribute(StageExecutionResult.COMMAND, cmd);
      result.addExceptionAttribute(ex);
      getDispatcherJobId(result);
      return result;
    }
  }

  public String getWorkDir(Stage stage) {
    if (stage.getStageParameters().getWorkDir() != null) {
      return stage.getStageParameters().getWorkDir();
    } else {
      return "";
    }
  }

  public String getWorkFile(Stage stage, String prefix, String suffix) {
    String workDir = getWorkDir(stage);
    if (!workDir.isEmpty() && !workDir.endsWith("/")) {
      workDir += "/";
    }
    return workDir
        + "pipelite-"
        + prefix
        + "-"
        + stage.getPipelineName()
        + "_"
        + stage.getProcessId()
        + "_"
        + stage.getStageName()
        + "."
        + suffix;
  }
}
