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

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.StageEntity;
import pipelite.entity.StageOutEntity;
import pipelite.executor.SuccessSyncExecutor;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
@Transactional
class StageServiceHsqlTest {

  @Autowired StageService service;

  private final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  @Test
  public void testCrud() {
    String stageName = UniqueStringGenerator.randomStageName();
    Stage stage = Stage.builder().stageName(stageName).executor(new SuccessSyncExecutor()).build();

    // Create.

    StageEntity stageEntity = service.beforeExecution(PIPELINE_NAME, PROCESS_ID, stage).get();
    assertThat(stageEntity).isEqualTo(stageEntity);
    StageOutEntity stageOutEntity = StageOutEntity.startExecution(stageEntity);
    service.saveStageOut(stageOutEntity);
    assertThat(service.getSavedStageOut(stageEntity).get()).isEqualTo(stageOutEntity);

    // Update.

    StageExecutionResult result = new StageExecutionResult(StageExecutionResultType.ERROR);
    result.setStdout("TEST3");
    result.setStderr("TEST4");
    stageEntity.endExecution(result);
    stageOutEntity = StageOutEntity.endExecution(stageEntity, result);

    service.saveStage(stageEntity);
    service.saveStageOut(stageOutEntity);

    assertThat(service.getSavedStage(PIPELINE_NAME, PROCESS_ID, stageName).get())
        .isEqualTo(stageEntity);
    StageOutEntity savedStageOutEntity = service.getSavedStageOut(stageEntity).get();
    assertThat(savedStageOutEntity).isEqualTo(stageOutEntity);
    assertThat(savedStageOutEntity.getStdOut()).isEqualTo("TEST3");
    assertThat(savedStageOutEntity.getStdErr()).isEqualTo("TEST4");

    // Delete.

    service.delete(stageEntity);

    assertThat(service.getSavedStage(PIPELINE_NAME, PROCESS_ID, stageName).isPresent()).isFalse();
    assertThat(service.getSavedStageOut(stageEntity).isPresent()).isFalse();
  }
}
