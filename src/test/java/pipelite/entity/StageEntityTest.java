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
package pipelite.entity;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.AbstractExecutor;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

class StageEntityTest {

  public static final class TestExecutor extends AbstractExecutor<ExecutorParameters>
      implements JsonSerializableExecutor {
    private static final String test = "TEST_EXECUTOR_DATA";

    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
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
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    String stageName = UniqueStringGenerator.randomStageName(this.getClass());

    TestExecutor executor = new TestExecutor();
    executor.setExecutorParams(
        ExecutorParameters.builder()
            .timeout(Duration.ofSeconds(0))
            .immediateRetries(0)
            .maximumRetries(0)
            .build());

    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    // Create execution.

    StageEntity stageEntity = StageEntity.createExecution(pipelineName, processId, stage);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName()).isNull();
    assertThat(stageEntity.getExecutorData()).isNull();
    assertThat(stageEntity.getExecutorParams()).isNull();

    // Start first execution.

    stageEntity.startExecution(stage);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ACTIVE);
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
                + "  \"immediateRetries\" : 0\n"
                + "}");

    // End first execution.

    StageExecutorResult firstExecutionResult = new StageExecutorResult(StageState.ERROR);
    stageEntity.endExecution(firstExecutionResult);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
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
                + "  \"immediateRetries\" : 0\n"
                + "}");

    // Start second execution.

    stageEntity.startExecution(stage);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ACTIVE);
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
                + "  \"immediateRetries\" : 0\n"
                + "}");

    // End second execution.

    StageExecutorResult secondExecutionResult = new StageExecutorResult(StageState.SUCCESS);
    stageEntity.endExecution(secondExecutionResult);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(2);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
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
                + "  \"immediateRetries\" : 0\n"
                + "}");

    // Reset execution.

    stageEntity.resetExecution();

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName()).isNull();
    assertThat(stageEntity.getExecutorData()).isNull();
    assertThat(stageEntity.getExecutorParams()).isNull();
  }
}
