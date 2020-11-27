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

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.SuccessSyncExecutor;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;

class StageOutEntityTest {

  @Test
  public void lifecycle() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    String stageName = UniqueStringGenerator.randomStageName();

    Stage stage = Stage.builder().stageName(stageName).executor(new SuccessSyncExecutor()).build();

    // Create execution.

    StageEntity stageEntity = StageEntity.createExecution(pipelineName, processId, stage);
    StageOutEntity stageOutEntity = StageOutEntity.startExecution(stageEntity);

    assertThat(stageOutEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageOutEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageOutEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageOutEntity.getStdOut()).isNull();
    assertThat(stageOutEntity.getStdErr()).isNull();

    // End execution.

    StageExecutionResult result = new StageExecutionResult(StageExecutionResultType.ERROR);
    result.setStdout("TEST3");
    result.setStderr("TEST4");
    stageOutEntity = StageOutEntity.endExecution(stageEntity, result);
    assertThat(stageOutEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageOutEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageOutEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageOutEntity.getStdOut()).isEqualTo("TEST3");
    assertThat(stageOutEntity.getStdErr()).isEqualTo("TEST4");

    // Reset execution.

    stageOutEntity = StageOutEntity.resetExecution(stageEntity);
    assertThat(stageOutEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageOutEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageOutEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageOutEntity.getStdOut()).isNull();
    assertThat(stageOutEntity.getStdErr()).isNull();
  }
}
