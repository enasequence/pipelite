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
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.UniqueStringGenerator;
import pipelite.entity.StageEntity;
import pipelite.entity.StageLogEntity;
import pipelite.executor.AbstractExecutor;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=StageServiceTest"})
@DirtiesContext
@ActiveProfiles("test")
@Transactional
class StageServiceTest {

  @Autowired StageService service;

  public static class TestExecutor extends AbstractExecutor implements JsonSerializableExecutor {

    private final StageState stageState;

    public TestExecutor(StageState stageState) {
      this.stageState = stageState;
    }

    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      return null;
    }

    @Override
    public void terminate() {}

    public StageState getStageState() {
      return stageState;
    }
  }

  @Test
  public void lifecycle() {

    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    String stageName = UniqueStringGenerator.randomStageName(this.getClass());

    TestExecutor executor = new TestExecutor(StageState.SUCCESS);
    executor.setExecutorParams(
        ExecutorParameters.builder()
            .immediateRetries(0)
            .maximumRetries(1)
            .timeout(Duration.ofSeconds(0))
            .build());

    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    // Create execution.

    service.createExecution(pipelineName, processId, stage);
    StageEntity stageEntity = stage.getStageEntity();

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

    service.startExecution(stage);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ACTIVE);
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.service.StageServiceTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"stageState\" : \"SUCCESS\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 1,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"logTimeout\" : 10000\n"
                + "}");

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);

    assertThat(service.getSavedStageLog(pipelineName, processId, stageName)).isNotPresent();

    // End first execution.

    StageExecutorResult firstExecutionResult = StageExecutorResult.error();
    service.endExecution(stage, firstExecutionResult);

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
        .isEqualTo("pipelite.service.StageServiceTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"stageState\" : \"SUCCESS\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 1,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"logTimeout\" : 10000\n"
                + "}");

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);

    Optional<StageLogEntity> stageLogEntity =
        service.getSavedStageLog(pipelineName, processId, stageName);
    assertThat(stageLogEntity).isPresent();

    // Start second execution.

    service.startExecution(stage);

    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ACTIVE);
    assertThat(stageEntity.getResultParams()).isNull();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNull();
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.service.StageServiceTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"stageState\" : \"SUCCESS\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 1,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"logTimeout\" : 10000\n"
                + "}");

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);

    assertThat(service.getSavedStageLog(pipelineName, processId, stageName)).isNotPresent();

    // End second execution.

    StageExecutorResult secondExecutionResult = StageExecutorResult.success();
    service.endExecution(stage, secondExecutionResult);

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
        .isEqualTo("pipelite.service.StageServiceTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"stageState\" : \"SUCCESS\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 0,\n"
                + "  \"maximumRetries\" : 1,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"logTimeout\" : 10000\n"
                + "}");

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);

    stageLogEntity = service.getSavedStageLog(pipelineName, processId, stageName);
    assertThat(stageLogEntity).isPresent();

    // Reset execution.

    service.resetExecution(stage);

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

    assertThat(service.getSavedStage(pipelineName, processId, stageName).get())
        .isEqualTo(stageEntity);

    assertThat(service.getSavedStageLog(pipelineName, processId, stageName)).isNotPresent();
  }
}
