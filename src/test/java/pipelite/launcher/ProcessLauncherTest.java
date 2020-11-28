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
package pipelite.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.stage.StageExecutionResultType.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.configuration.StageConfiguration;
import pipelite.entity.StageEntity;
import pipelite.executor.StageExecutorParameters;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

public class ProcessLauncherTest {

  private void assertMaximumRetries(Integer maximumRetries, int expectedMaximumRetries) {
    assertThat(
            ProcessLauncher.getMaximumRetries(
                Stage.builder()
                    .stageName("STAGE")
                    .executor((pipelineName, processId, stage) -> StageExecutionResult.success())
                    .executorParams(
                        StageExecutorParameters.builder().maximumRetries(maximumRetries).build())
                    .build()))
        .isEqualTo(expectedMaximumRetries);
  }

  private void assertImmediateRetries(
      Integer immediateRetries, Integer maximumRetries, int expectedImmediateRetries) {
    assertThat(
            ProcessLauncher.getImmediateRetries(
                Stage.builder()
                    .stageName("STAGE")
                    .executor((pipelineName, processId, stage) -> StageExecutionResult.success())
                    .executorParams(
                        StageExecutorParameters.builder()
                            .maximumRetries(maximumRetries)
                            .immediateRetries(immediateRetries)
                            .build())
                    .build()))
        .isEqualTo(expectedImmediateRetries);
  }

  @Test
  public void maximumRetries() {
    assertMaximumRetries(1, 1);
    assertMaximumRetries(5, 5);
    assertMaximumRetries(null, StageConfiguration.DEFAULT_MAX_RETRIES);
  }

  @Test
  public void immediateRetries() {
    assertImmediateRetries(3, 6, 3);
    assertImmediateRetries(3, 2, 2);
    assertImmediateRetries(3, 0, 0);
    assertImmediateRetries(
        null,
        StageConfiguration.DEFAULT_IMMEDIATE_RETRIES + 1,
        StageConfiguration.DEFAULT_IMMEDIATE_RETRIES);
    assertImmediateRetries(null, null, StageConfiguration.DEFAULT_IMMEDIATE_RETRIES);
  }

  public static List<Stage> createStageExecutions(
      StageExecutionResultType firstStageState,
      StageExecutionResultType secondStageState,
      int executions,
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
    firstStageEntity.setExecutionCount(executions);
    for (int i = 0; i < executions; ++i) {
      firstStage.incrementImmediateExecutionCount();
    }
    stages.add(firstStage);

    Stage secondStage = process.getStages().get(1);
    StageEntity secondStageEntity = new StageEntity();
    secondStage.setStageEntity(secondStageEntity);
    secondStageEntity.setResultType(secondStageState);
    secondStageEntity.setExecutionCount(executions);
    for (int i = 0; i < executions; ++i) {
      secondStage.incrementImmediateExecutionCount();
    }
    stages.add(secondStage);

    return stages;
  }

  private void evaluateProcessStateNoRetries(
      StageExecutionResultType firstStageState,
      StageExecutionResultType secondStageState,
      ProcessState state) {
    List<Stage> stages = createStageExecutions(firstStageState, secondStageState, 1, 0, 0);
    assertThat(ProcessLauncher.evaluateProcessState(stages)).isEqualTo(state);
  }

  @Test
  public void evaluateProcessStateNoRetries() {
    evaluateProcessStateNoRetries(SUCCESS, null, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(SUCCESS, SUCCESS, ProcessState.COMPLETED);
    evaluateProcessStateNoRetries(SUCCESS, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(SUCCESS, ERROR, ProcessState.FAILED);

    evaluateProcessStateNoRetries(ACTIVE, null, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, SUCCESS, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ACTIVE, ERROR, ProcessState.ACTIVE);

    evaluateProcessStateNoRetries(ERROR, null, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ERROR, SUCCESS, ProcessState.FAILED);
    evaluateProcessStateNoRetries(ERROR, ACTIVE, ProcessState.ACTIVE);
    evaluateProcessStateNoRetries(ERROR, ERROR, ProcessState.FAILED);
  }

  private void evaluateProcessStateWithRetries(
      StageExecutionResultType firstStageState,
      StageExecutionResultType secondStageState,
      ProcessState state) {
    List<Stage> stages = createStageExecutions(firstStageState, secondStageState, 1, 1, 1);
    assertThat(ProcessLauncher.evaluateProcessState(stages)).isEqualTo(state);
  }

  @Test
  public void evaluateProcessStateWithRetries() {
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
