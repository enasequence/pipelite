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

import org.junit.jupiter.api.Test;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.test.PipeliteTestIdCreator;

class StageLogEntityTest {

  @Test
  public void lifecycle() {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();
    String stageName = PipeliteTestIdCreator.stageName();

    Stage stage =
        Stage.builder()
            .stageName(stageName)
            .executor(StageExecutor.createSyncTestExecutor(StageExecutorState.SUCCESS, null))
            .build();

    // Create execution.

    stage.setStageEntity(
        StageEntity.createExecution(pipelineName, processId, stage.getStageName()));

    // End execution.

    StageExecutorResult result = StageExecutorResult.executionError();
    result.stdOut("TEST3");
    StageLogEntity stageLogEntity = StageLogEntity.endExecution(stage.getStageEntity(), result);
    assertThat(stageLogEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageLogEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageLogEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageLogEntity.getStageLog()).isEqualTo("TEST3");
  }
}
