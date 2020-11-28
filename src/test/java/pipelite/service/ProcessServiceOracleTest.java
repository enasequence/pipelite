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

import java.util.Comparator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.process.ProcessState;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"oracle-test", "pipelite-test"})
@Transactional
class ProcessServiceOracleTest {

  @Autowired ProcessService service;

  @Test
  public void testCrud() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    Integer priority = 0;

    ProcessEntity processEntity = ProcessEntity.startExecution(pipelineName, processId, priority);

    service.saveProcess(processEntity);
    assertThat(service.getSavedProcess(pipelineName, processId).get()).isEqualTo(processEntity);

    processEntity.updateExecution(ProcessState.COMPLETED);

    service.saveProcess(processEntity);
    assertThat(service.getSavedProcess(pipelineName, processId).get()).isEqualTo(processEntity);

    service.delete(processEntity);

    assertThat(service.getSavedProcess(pipelineName, processId).isPresent()).isFalse();
  }

  @Test
  public void testGetProcessesSamePriority() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();

    service.saveProcess(create(pipelineName, ProcessState.ACTIVE, 1));
    service.saveProcess(create(pipelineName, ProcessState.ACTIVE, 1));
    service.saveProcess(create(pipelineName, ProcessState.COMPLETED, 1));
    service.saveProcess(create(pipelineName, ProcessState.COMPLETED, 1));
    service.saveProcess(create(pipelineName, ProcessState.COMPLETED, 1));
    service.saveProcess(create(pipelineName, ProcessState.FAILED, 1));
    service.saveProcess(create(pipelineName, ProcessState.FAILED, 1));
    service.saveProcess(create(pipelineName, ProcessState.FAILED, 1));
    service.saveProcess(create(pipelineName, ProcessState.FAILED, 1));

    assertThat(service.getActiveProcesses(pipelineName)).hasSize(2);
    assertThat(service.getCompletedProcesses(pipelineName)).hasSize(3);
    assertThat(service.getFailedProcesses(pipelineName)).hasSize(4);
  }

  @Test
  public void testGetProcessesDifferentPriority() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();

    service.saveProcess(create(pipelineName, ProcessState.ACTIVE, 1));
    service.saveProcess(create(pipelineName, ProcessState.ACTIVE, 2));
    service.saveProcess(create(pipelineName, ProcessState.COMPLETED, 1));
    service.saveProcess(create(pipelineName, ProcessState.COMPLETED, 2));
    service.saveProcess(create(pipelineName, ProcessState.COMPLETED, 3));
    service.saveProcess(create(pipelineName, ProcessState.FAILED, 1));
    service.saveProcess(create(pipelineName, ProcessState.FAILED, 2));
    service.saveProcess(create(pipelineName, ProcessState.FAILED, 4));
    service.saveProcess(create(pipelineName, ProcessState.FAILED, 3));

    assertThat(service.getActiveProcesses(pipelineName)).hasSize(2);
    assertThat(service.getCompletedProcesses(pipelineName)).hasSize(3);
    assertThat(service.getFailedProcesses(pipelineName)).hasSize(4);

    assertThat(service.getActiveProcesses(pipelineName))
        .isSortedAccordingTo(Comparator.comparingInt(ProcessEntity::getPriority).reversed());
    assertThat(service.getFailedProcesses(pipelineName))
        .isSortedAccordingTo(Comparator.comparingInt(ProcessEntity::getPriority).reversed());
  }

  private static ProcessEntity create(String pipelineName, ProcessState state, int priority) {
    ProcessEntity processEntity =
        ProcessEntity.startExecution(
            pipelineName, UniqueStringGenerator.randomProcessId(), priority);
    processEntity.updateExecution(state);
    return processEntity;
  }
}
