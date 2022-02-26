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

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.TestExecutor;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;

class StageLogEntityTest {

  @Test
  public void lifecycle() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    String stageName = UniqueStringGenerator.randomStageName();

    Stage stage =
        Stage.builder()
            .stageName(stageName)
            .executor(TestExecutor.sync(StageExecutorState.SUCCESS))
            .build();

    // Create execution.

    StageEntity.createExecution(pipelineName, processId, stage);

    // End execution.

    StageExecutorResult result = StageExecutorResult.error();
    result.setStageLog("TEST3");
    StageLogEntity stageLogEntity = StageLogEntity.endExecution(stage.getStageEntity(), result);
    assertThat(stageLogEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageLogEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageLogEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageLogEntity.getStageLog()).isEqualTo("TEST3");
  }
}
