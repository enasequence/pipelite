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
package pipelite.stage;

import com.google.common.flogger.FluentLogger;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.executor.StageExecutor;
import pipelite.log.LogKey;

@Flogger
@Value
@Builder
public class Stage {
  private final String pipelineName;
  private final String processId;
  private final String stageName;
  @EqualsAndHashCode.Exclude private final StageExecutor executor;
  @EqualsAndHashCode.Exclude private final Stage dependsOn;
  @EqualsAndHashCode.Exclude private final StageParameters stageParameters;

  public Stage(
      String pipelineName,
      String processId,
      String stageName,
      StageExecutor executor,
      Stage dependsOn,
      StageParameters stageParameters) {
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.stageName = stageName;
    this.executor = executor;
    this.dependsOn = dependsOn;
    if (stageParameters != null) {
      this.stageParameters = stageParameters;
    } else {
      this.stageParameters = StageParameters.builder().build();
    }
  }

  public boolean validate() {
    boolean isSuccess = true;
    if (pipelineName == null || pipelineName.isEmpty()) {
      logContext(log.atSevere()).log("Pipeline name is missing");
      isSuccess = false;
    }
    if (processId == null || processId.isEmpty()) {
      logContext(log.atSevere()).log("Process id is missing");
      isSuccess = false;
    }
    if (stageName == null || stageName.isEmpty()) {
      logContext(log.atSevere()).log("Stage name is missing");
      isSuccess = false;
    }
    if (executor == null) {
      logContext(log.atSevere()).log("Executor is missing");
      isSuccess = false;
    }
    if (stageParameters == null) {
      logContext(log.atSevere()).log("Stage parameters are missing");
      isSuccess = false;
    }
    return isSuccess;
  }

  public StageExecutionResult execute() {
    return executor.execute(this);
  }

  public FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stageName);
  }
}