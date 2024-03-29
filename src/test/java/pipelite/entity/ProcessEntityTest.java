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

import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import pipelite.process.ProcessState;
import pipelite.test.PipeliteTestIdCreator;

class ProcessEntityTest {

  @Test
  public void lifecycle() {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();
    int priority = 1;

    ProcessEntity processEntity = ProcessEntity.createExecution(pipelineName, processId, priority);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getCreateTime()).isNotNull();
    ZonedDateTime createTime = processEntity.getCreateTime();
    assertThat(processEntity.getStartTime()).isNull();
    assertThat(processEntity.getEndTime()).isNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.PENDING);

    processEntity.startExecution();

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getCreateTime()).isEqualTo(createTime);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.ACTIVE);

    processEntity.endExecution(ProcessState.COMPLETED);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getCreateTime()).isEqualTo(createTime);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNotNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
  }

  @Test
  public void priority() {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();

    assertThat(ProcessEntity.getBoundedPriority(null)).isEqualTo(ProcessEntity.DEFAULT_PRIORITY);
    assertThat(ProcessEntity.getBoundedPriority(ProcessEntity.MIN_PRIORITY - 1))
        .isEqualTo(ProcessEntity.MIN_PRIORITY);
    assertThat(ProcessEntity.getBoundedPriority(ProcessEntity.MAX_PRIORITY + 1))
        .isEqualTo(ProcessEntity.MAX_PRIORITY);

    ProcessEntity processEntity = ProcessEntity.createExecution(pipelineName, processId, null);
    assertThat(processEntity.getPriority()).isEqualTo(ProcessEntity.DEFAULT_PRIORITY);
    processEntity =
        ProcessEntity.createExecution(pipelineName, processId, ProcessEntity.MIN_PRIORITY - 1);
    assertThat(processEntity.getPriority()).isEqualTo(ProcessEntity.MIN_PRIORITY);
    processEntity =
        ProcessEntity.createExecution(pipelineName, processId, ProcessEntity.MAX_PRIORITY + 1);
    assertThat(processEntity.getPriority()).isEqualTo(ProcessEntity.MAX_PRIORITY);
  }
}
