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

import lombok.extern.flogger.Flogger;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.CommandRunnerResult;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCode;
import pipelite.task.TaskInstance;

@Flogger
public abstract class CommandExecutor implements TaskExecutor {

  public abstract CommandRunner getCmdRunner();

  public abstract String getCmd(TaskInstance taskInstance);

  public String getDispatcherCmd(TaskInstance taskInstance) {
    return null;
  }

  public void getDispatcherJobId(TaskExecutionResult taskExecutionResult) {}

  public TaskExecutionResult execute(TaskInstance taskInstance) {
    String cmd = getCmd(taskInstance);
    String dispatchCommand = getDispatcherCmd(taskInstance);
    if (dispatchCommand != null) {
      cmd = dispatchCommand + " " + cmd;
    }

    try {
      CommandRunnerResult commandRunnerResult =
          getCmdRunner().execute(cmd, taskInstance.getTaskParameters());

      TaskExecutionResult result =
          TaskExecutionResultExitCode.resolve(commandRunnerResult.getExitCode());
      result.addAttribute(TaskExecutionResult.COMMAND, cmd);
      result.addAttribute(TaskExecutionResult.HOST, taskInstance.getTaskParameters().getHost());
      result.addAttribute(TaskExecutionResult.EXIT_CODE, commandRunnerResult.getExitCode());
      result.setStdout(commandRunnerResult.getStdout());
      result.setStderr(commandRunnerResult.getStderr());
      return result;

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed call: %s", cmd);
      TaskExecutionResult result = TaskExecutionResult.error();
      result.addAttribute(TaskExecutionResult.COMMAND, cmd);
      result.addExceptionAttribute(ex);
      getDispatcherJobId(result);
      return result;
    }
  }

  public String getWorkDir(TaskInstance taskInstance) {
    if (taskInstance.getTaskParameters().getWorkDir() != null) {
      return taskInstance.getTaskParameters().getWorkDir();
    } else {
      return "";
    }
  }

  public String getWorkFile(TaskInstance taskInstance, String prefix, String suffix) {
    String workDir = getWorkDir(taskInstance);
    if (!workDir.isEmpty() && !workDir.endsWith("/")) {
      workDir += "/";
    }
    return workDir
        + "pipelite-"
        + prefix
        + "-"
        + taskInstance.getProcessName()
        + "_"
        + taskInstance.getProcessId()
        + "_"
        + taskInstance.getTaskName()
        + "."
        + suffix;
  }
}
