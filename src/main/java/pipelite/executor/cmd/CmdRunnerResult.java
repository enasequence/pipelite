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

import static pipelite.executor.cmd.CmdRunner.EXIT_CODE_SUCCESS;

import lombok.Builder;
import lombok.Getter;
import pipelite.stage.executor.InternalError;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;

@Getter
@Builder
public class CmdRunnerResult {
  private final int exitCode;
  private final String stdout;
  private final String stderr;
  private final InternalError internalError;

  public CmdRunnerResult(int exitCode, String stdout, String stderr, InternalError internalError) {
    this.exitCode = exitCode;
    this.stdout = stdout;
    this.stderr = stderr;
    this.internalError = internalError;
  }

  public StageExecutorResult getStageExecutorResult(String cmd) {
    StageExecutorResult result;
    if (exitCode == EXIT_CODE_SUCCESS) {
      result = StageExecutorResult.success();
    } else {
      result = StageExecutorResult.error();
    }
    result.setStdout(stdout);
    result.setStderr(stderr);
    result.setInternalError(internalError);
    result.addAttribute(StageExecutorResultAttribute.EXIT_CODE, exitCode);
    result.addAttribute(StageExecutorResultAttribute.COMMAND, cmd);
    return result;
  }
}
