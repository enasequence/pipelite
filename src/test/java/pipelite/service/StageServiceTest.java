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
package pipelite.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.StageEntity;
import pipelite.executor.AbstractExecutor;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultType;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@Transactional
class StageServiceTest {

  @Autowired StageService service;

  public static class TestExecutor extends AbstractExecutor implements JsonSerializableExecutor {

    private final StageExecutorResultType resultType;

    public TestExecutor(StageExecutorResultType resultType) {
      this.resultType = resultType;
    }

    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      return null;
    }

    @Override
    public void terminate() {}

    public StageExecutorResultType getResultType() {
      return resultType;
    }
  }

  @Test
  public void lifecycle() {

    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    String stageName = UniqueStringGenerator.randomStageName();

    TestExecutor executor = new TestExecutor(StageExecutorResultType.SUCCESS);
    executor.setExecutorParams(
        ExecutorParameters.builder()
            .immediateRetries(0)
            .maximumRetries(1)
            .timeout(Duration.ofSeconds(0))
            .build());

    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    // Create execution.

    StageEntity stageEntity = service.createExecution(pipelineName, processId, stage);

    stage.setStageEntity(stageEntity);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getResultType()).isNull();
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName()).isNull();
    assertThat(stageEntity.getExecutorData()).isNull();
    assertThat(stageEntity.getExecutorParams()).isNull();

    // Start first execution.

    service.startExecution(stage);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getResultType()).isEqualTo(StageExecutorResultType.ACTIVE);
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.service.StageServiceTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"resultType\" : \"SUCCESS\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 1,\n"
                + "  \"immediateRetries\" : 0\n"
                + "}");

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);

    // End first execution.

    StageExecutorResult firstExecutionResult =
        new StageExecutorResult(StageExecutorResultType.ERROR);
    service.endExecution(stage, firstExecutionResult);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getResultType()).isEqualTo(StageExecutorResultType.ERROR);
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNotNull();
    assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.service.StageServiceTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"resultType\" : \"SUCCESS\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 1,\n"
                + "  \"immediateRetries\" : 0\n"
                + "}");

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);

    // Start second execution.

    service.startExecution(stage);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getResultType()).isEqualTo(StageExecutorResultType.ACTIVE);
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.service.StageServiceTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"resultType\" : \"SUCCESS\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 1,\n"
                + "  \"immediateRetries\" : 0\n"
                + "}");

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);

    // End second execution.

    StageExecutorResult secondExecutionResult =
        new StageExecutorResult(StageExecutorResultType.SUCCESS);
    service.endExecution(stage, secondExecutionResult);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(2);
    assertThat(stageEntity.getResultType()).isEqualTo(StageExecutorResultType.SUCCESS);
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNotNull();
    assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.service.StageServiceTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"resultType\" : \"SUCCESS\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 1,\n"
                + "  \"immediateRetries\" : 0\n"
                + "}");

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);

    // Reset execution.

    service.resetExecution(stage);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getResultType()).isNull();
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName()).isNull();
    assertThat(stageEntity.getExecutorData()).isNull();
    assertThat(stageEntity.getExecutorParams()).isNull();

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);
  }
}