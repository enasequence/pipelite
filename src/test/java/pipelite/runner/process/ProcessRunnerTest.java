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
package pipelite.runner.process;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.stage.StageState.*;

import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import pipelite.entity.StageEntity;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.parameters.ExecutorParameters;

public class ProcessRunnerTest {

  @Test
  public void evaluateProcessStateNoRetries() {
    evaluateProcessStateNoRetries(PENDING, PENDING, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(PENDING, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(PENDING, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(PENDING, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(SUCCESS, PENDING, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(SUCCESS, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(SUCCESS, ERROR, ProcessState.FAILED);
    evaluateProcessStateNoRetries(SUCCESS, SUCCESS, ProcessState.COMPLETED);
    evaluateProcessStateNoRetries(ACTIVE, PENDING, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ERROR, PENDING, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ERROR, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ERROR, ERROR, ProcessState.FAILED);
    evaluateProcessStateNoRetries(ERROR, SUCCESS, ProcessState.FAILED);
  }

  @Test
  public void evaluateProcessStateWithRetries() {
    evaluateProcessStateWithRetries(PENDING, PENDING, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(PENDING, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(PENDING, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(PENDING, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(SUCCESS, PENDING, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(SUCCESS, SUCCESS, ProcessState.COMPLETED);
    evaluateProcessStateWithRetries(SUCCESS, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(SUCCESS, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, PENDING, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ACTIVE, ERROR, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, PENDING, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateWithRetries(ERROR, ERROR, ProcessState.ACTIVE);
  }

  private void evaluateProcessStateNoRetries(
      StageState firstStageState, StageState secondStageState, ProcessState state) {
    int firstStageExecutions = firstStageState != null ? 1 : 0;
    int secondStageExecutions = secondStageState != null ? 1 : 0;
    Process process =
        twoIndependentStagesProcess(
            firstStageState, secondStageState, firstStageExecutions, secondStageExecutions, 0, 0);
    Assertions.assertThat(ProcessRunner.evaluateProcessState(process)).isEqualTo(state);
  }

  private void evaluateProcessStateWithRetries(
      StageState firstStageState, StageState secondStageState, ProcessState state) {
    int firstStageExecutions = firstStageState != null ? 1 : 0;
    int secondStageExecutions = secondStageState != null ? 1 : 0;
    Process process =
        twoIndependentStagesProcess(
            firstStageState, secondStageState, firstStageExecutions, secondStageExecutions, 1, 1);
    assertThat(ProcessRunner.evaluateProcessState(process)).isEqualTo(state);
  }

  public static Process twoIndependentStagesProcess(
      StageState firstStageState,
      StageState secondStageState,
      int firstStageExecutions,
      int secondStageExecutions,
      int maximumRetries,
      int immediateRetries) {
    ExecutorParameters executorParams =
        ExecutorParameters.builder()
            .maximumRetries(maximumRetries)
            .immediateRetries(immediateRetries)
            .build();
    Process process =
        new ProcessBuilder("pipelite-test")
            .execute("STAGE0")
            .withSyncTestExecutor((request) -> null, executorParams)
            .execute("STAGE1")
            .withSyncTestExecutor((request) -> null, executorParams)
            .build();
    List<Stage> stages = new ArrayList<>();

    Stage firstStage = process.getStage("STAGE0").get();
    StageEntity firstStageEntity = new StageEntity();
    firstStage.setStageEntity(firstStageEntity);
    firstStageEntity.setStageState(firstStageState);
    firstStageEntity.setExecutionCount(firstStageExecutions);
    for (int i = 0; i < firstStageExecutions; ++i) {
      firstStage.incrementImmediateExecutionCount();
    }
    stages.add(firstStage);

    Stage secondStage = process.getStage("STAGE1").get();
    StageEntity secondStageEntity = new StageEntity();
    secondStage.setStageEntity(secondStageEntity);
    secondStageEntity.setStageState(secondStageState);
    secondStageEntity.setExecutionCount(secondStageExecutions);
    for (int i = 0; i < secondStageExecutions; ++i) {
      secondStage.incrementImmediateExecutionCount();
    }
    stages.add(secondStage);

    return process;
  }
}
