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
import pipelite.stage.executor.EmptySyncStageExecutor;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultType;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"oracle-test", "pipelite-test"})
@Transactional
class StageServiceOracleTest {

  @Autowired StageService service;

  private final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  @Test
  public void testCrud() {
    String stageName = UniqueStringGenerator.randomStageName();
    Stage stage =
        Stage.builder()
            .stageName(stageName)
            .executor(new EmptySyncStageExecutor(StageExecutorResultType.SUCCESS))
            .build();

    // Create.

    StageEntity stageEntity = StageEntity.createExecution(PIPELINE_NAME, PROCESS_ID, stage);
    StageOutEntity stageOutEntity = StageOutEntity.startExecution(stageEntity);

    service.saveStage(stageEntity);
    service.saveStageOut(stageOutEntity);

    assertThat(service.getSavedStage(PIPELINE_NAME, PROCESS_ID, stageName).get())
        .isEqualTo(stageEntity);
    assertThat(service.getSavedStageOut(stageEntity).get()).isEqualTo(stageOutEntity);

    // Update.

    StageExecutorResult result = new StageExecutorResult(StageExecutorResultType.ERROR);
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
