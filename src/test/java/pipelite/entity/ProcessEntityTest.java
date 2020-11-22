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
import pipelite.process.ProcessState;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessEntityTest {

  @Test
  public void newExecution() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    int priority = 1;
    ProcessEntity processEntity = ProcessEntity.newExecution(pipelineName, processId, priority);
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getState()).isEqualTo(ProcessState.NEW);
  }
}
