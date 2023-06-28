/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.entity;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.entity.field.ErrorType;
import pipelite.entity.field.StageState;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.executor.SyncExecutor;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.test.PipeliteTestIdCreator;

class StageEntityTest {

  public static final class TestExecutor extends SyncExecutor<ExecutorParameters>
      implements JsonSerializableExecutor {
    private static final String test = "TEST_EXECUTOR_DATA";

    @Override
    public StageExecutorResult execute() {
      return null;
    }

    @Override
    public void terminate() {}

    public String getTest() {
      return test;
    }
  }

  @Test
  public void lifecycle() {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();
    String stageName = PipeliteTestIdCreator.stageName();

    ExecutorParameters executorParams =
        ExecutorParameters.builder()
            .timeout(Duration.ofSeconds(0))
            .immediateRetries(0)
            .maximumRetries(0)
            .build();

    TestExecutor executor = new TestExecutor();
    executor.setExecutorParams(executorParams);

    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    // Create execution.

    stage.setStageEntity(
        StageEntity.createExecution(pipelineName, processId, stage.getStageName()));
    StageEntity stageEntity = stage.getStageEntity();
    StageService.prepareSaveStage(stage);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
    assertThat(stageEntity.getErrorType()).isNull();
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.entity.StageEntityTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"test\" : \"TEST_EXECUTOR_DATA\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 0,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"logSave\" : \"ERROR\",\n"
                + "  \"logLines\" : 1000\n"
                + "}");

    // Start first execution.

    stageEntity.startExecution();

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ACTIVE);
    assertThat(stageEntity.getErrorType()).isNull();
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.entity.StageEntityTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"test\" : \"TEST_EXECUTOR_DATA\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 0,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"logSave\" : \"ERROR\",\n"
                + "  \"logLines\" : 1000\n"
                + "}");

    // End first execution.

    StageExecutorResult firstExecutionResult = StageExecutorResult.executionError();
    stageEntity.endExecution(firstExecutionResult, executorParams.getPermanentErrors());

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
    assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.EXECUTION_ERROR);
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNotNull();
    assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.entity.StageEntityTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"test\" : \"TEST_EXECUTOR_DATA\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 0,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"logSave\" : \"ERROR\",\n"
                + "  \"logLines\" : 1000\n"
                + "}");

    // Start second execution.

    stageEntity.startExecution();

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ACTIVE);
    assertThat(stageEntity.getErrorType()).isNull();
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.entity.StageEntityTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"test\" : \"TEST_EXECUTOR_DATA\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 0,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"logSave\" : \"ERROR\",\n"
                + "  \"logLines\" : 1000\n"
                + "}");

    // End second execution.

    StageExecutorResult secondExecutionResult = StageExecutorResult.success();
    stageEntity.endExecution(secondExecutionResult, executorParams.getPermanentErrors());

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(2);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
    assertThat(stageEntity.getErrorType()).isNull();
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNotNull();
    assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.entity.StageEntityTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"test\" : \"TEST_EXECUTOR_DATA\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 0,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"logSave\" : \"ERROR\",\n"
                + "  \"logLines\" : 1000\n"
                + "}");

    // Reset execution.

    stageEntity.resetExecution();

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
    assertThat(stageEntity.getErrorType()).isNull();
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName()).isNull();
    assertThat(stageEntity.getExecutorData()).isNull();
    assertThat(stageEntity.getExecutorParams()).isNull();
  }

  @Test
  public void errorType() {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();
    String stageName = PipeliteTestIdCreator.stageName();

    TestExecutor executor = new TestExecutor();
    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    // Execution error
    StageEntity stageEntity = initErrorTypeTest(pipelineName, processId, stage);
    stageEntity.endExecution(
        StageExecutorResult.create(StageExecutorState.EXECUTION_ERROR), Collections.emptyList());
    assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.EXECUTION_ERROR);

    // Permanent error
    stageEntity = initErrorTypeTest(pipelineName, processId, stage);
    StageExecutorResult permanentError = StageExecutorResult.executionError();
    permanentError.attribute(StageExecutorResultAttribute.EXIT_CODE, "1");
    stageEntity.endExecution(permanentError, List.of(1));
    assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.PERMANENT_ERROR);

    // Timeout error
    stageEntity = initErrorTypeTest(pipelineName, processId, stage);
    stageEntity.endExecution(
        StageExecutorResult.create(StageExecutorState.TIMEOUT_ERROR), Collections.emptyList());
    assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.TIMEOUT_ERROR);

    // Memory error
    stageEntity = initErrorTypeTest(pipelineName, processId, stage);
    stageEntity.endExecution(
        StageExecutorResult.create(StageExecutorState.MEMORY_ERROR), Collections.emptyList());
    assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.MEMORY_ERROR);

    // Terminated error
    stageEntity = initErrorTypeTest(pipelineName, processId, stage);
    stageEntity.endExecution(
        StageExecutorResult.create(StageExecutorState.TERMINATED_ERROR), Collections.emptyList());
    assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.TERMINATED_ERROR);

    // Lost error
    stageEntity = initErrorTypeTest(pipelineName, processId, stage);
    stageEntity.endExecution(
        StageExecutorResult.create(StageExecutorState.LOST_ERROR), Collections.emptyList());
    assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.LOST_ERROR);

    // Internal error
    stageEntity = initErrorTypeTest(pipelineName, processId, stage);
    stageEntity.endExecution(
        StageExecutorResult.create(StageExecutorState.INTERNAL_ERROR), Collections.emptyList());
    assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.INTERNAL_ERROR);
  }

  private static StageEntity initErrorTypeTest(String pipelineName, String processId, Stage stage) {
    stage.setStageEntity(
        StageEntity.createExecution(pipelineName, processId, stage.getStageName()));
    StageEntity stageEntity = stage.getStageEntity();
    assertThat(stageEntity.getErrorType()).isNull();
    return stageEntity;
  }
}
