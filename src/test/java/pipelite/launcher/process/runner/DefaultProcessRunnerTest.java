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
package pipelite.launcher.process.runner;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.stage.executor.StageExecutorResultType.*;

import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import pipelite.entity.StageEntity;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorParameters;
import pipelite.stage.executor.StageExecutorResultType;

public class DefaultProcessRunnerTest {

  public static List<Stage> createStageExecutions(
      StageExecutorResultType firstStageState,
      StageExecutorResultType secondStageState,
      int firstStageExecutions,
      int secondStageExecutions,
      int maximumRetries,
      int immediateRetries) {
    StageExecutorParameters executorParams =
        StageExecutorParameters.builder()
            .maximumRetries(maximumRetries)
            .immediateRetries(immediateRetries)
            .build();
    Process process =
        new ProcessBuilder("pipelite-test")
            .execute("STAGE0", executorParams)
            .with((pipelineName, processId, stage) -> null)
            .execute("STAGE1", executorParams)
            .with((pipelineName, processId, stage) -> null)
            .build();
    List<Stage> stages = new ArrayList<>();

    Stage firstStage = process.getStages().get(0);
    StageEntity firstStageEntity = new StageEntity();
    firstStage.setStageEntity(firstStageEntity);
    firstStageEntity.setResultType(firstStageState);
    firstStageEntity.setExecutionCount(firstStageExecutions);
    for (int i = 0; i < firstStageExecutions; ++i) {
      firstStage.incrementImmediateExecutionCount();
    }
    stages.add(firstStage);

    Stage secondStage = process.getStages().get(1);
    StageEntity secondStageEntity = new StageEntity();
    secondStage.setStageEntity(secondStageEntity);
    secondStageEntity.setResultType(secondStageState);
    secondStageEntity.setExecutionCount(secondStageExecutions);
    for (int i = 0; i < secondStageExecutions; ++i) {
      secondStage.incrementImmediateExecutionCount();
    }
    stages.add(secondStage);

    return stages;
  }

  private void evaluateProcessStateNoRetries(
      StageExecutorResultType firstStageState,
      StageExecutorResultType secondStageState,
      ProcessState state) {
    int firstStageExecutions = firstStageState != null ? 1 : 0;
    int secondStageExecutions = secondStageState != null ? 1 : 0;
    List<Stage> stages =
        createStageExecutions(
            firstStageState, secondStageState, firstStageExecutions, secondStageExecutions, 0, 0);
    Assertions.assertThat(DefaultProcessRunner.evaluateProcessState(stages)).isEqualTo(state);
  }

  @Test
  public void evaluateProcessStateNoRetries() {
    evaluateProcessStateNoRetries(null, null, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(null, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(null, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(null, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(SUCCESS, null, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(SUCCESS, ACTIVE, ProcessState.FAILED);
    evaluateProcessStateNoRetries(SUCCESS, ERROR, ProcessState.FAILED);
    evaluateProcessStateNoRetries(SUCCESS, SUCCESS, ProcessState.COMPLETED);
    evaluateProcessStateNoRetries(ACTIVE, null, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, ACTIVE, ProcessState.FAILED);
    evaluateProcessStateNoRetries(ACTIVE, ERROR, ProcessState.FAILED);
    evaluateProcessStateNoRetries(ACTIVE, SUCCESS, ProcessState.FAILED);
    evaluateProcessStateNoRetries(ERROR, null, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ERROR, ACTIVE, ProcessState.FAILED);
    evaluateProcessStateNoRetries(ERROR, ERROR, ProcessState.FAILED);
    evaluateProcessStateNoRetries(ERROR, SUCCESS, ProcessState.FAILED);
  }

  private void evaluateProcessStateWithRetries(
      StageExecutorResultType firstStageState,
      StageExecutorResultType secondStageState,
      ProcessState state) {
    int firstStageExecutions = firstStageState != null ? 1 : 0;
    int secondStageExecutions = secondStageState != null ? 1 : 0;
    List<Stage> stages =
        createStageExecutions(
            firstStageState, secondStageState, firstStageExecutions, secondStageExecutions, 1, 1);
    assertThat(DefaultProcessRunner.evaluateProcessState(stages)).isEqualTo(state);
  }

  @Test
  public void evaluateProcessStateWithRetries() {
    evaluateProcessStateWithRetries(null, null, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(null, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(null, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(null, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(SUCCESS, null, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(SUCCESS, SUCCESS, ProcessState.COMPLETED);
    evaluateProcessStateWithRetries(SUCCESS, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(SUCCESS, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, null, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, null, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, ERROR, ProcessState.ACTIVE);
  }
}
