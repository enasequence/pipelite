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
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.FullTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.process.ProcessExecutionState;

@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test"})
class HsqlProcessServiceTest {

  @Autowired
  ProcessService service;

  @Test
  @Transactional
  @Rollback
  public void testCrud() {

    String processName = UniqueStringGenerator.randomProcessName();
    String processId = UniqueStringGenerator.randomProcessId();
    ProcessExecutionState state = ProcessExecutionState.ACTIVE;
    Integer execCnt = 3;
    Integer priority = 0;

    ProcessEntity process = new ProcessEntity(processId, processName, state, execCnt, priority);

    service.saveProcess(process);

    assertThat(service.getSavedProcess(processName, processId).get()).isEqualTo(process);

    process.setState(ProcessExecutionState.COMPLETED);
    process.setExecutionCount(4);
    process.setPriority(9);

    service.saveProcess(process);

    assertThat(service.getSavedProcess(processName, processId).get()).isEqualTo(process);

    service.delete(process);

    assertThat(service.getSavedProcess(processName, processId).isPresent()).isFalse();
  }

  @Test
  @Transactional
  @Rollback
  public void testReportsSamePriority() {
    String processName = UniqueStringGenerator.randomProcessName();

    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.ACTIVE, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.ACTIVE, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.FAILED, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.FAILED, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.FAILED, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.FAILED, 1));

    assertThat(service.getActiveProcesses(processName)).hasSize(2);
    assertThat(service.getCompletedProcesses(processName)).hasSize(3);
    assertThat(service.getFailedProcesses(processName)).hasSize(4);
  }

  @Test
  @Transactional
  @Rollback
  public void testReportsDifferentPriority() {
    String processName = UniqueStringGenerator.randomProcessName();

    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.ACTIVE, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.ACTIVE, 2));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.COMPLETED, 2));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.COMPLETED, 3));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.FAILED, 1));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.FAILED, 2));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.FAILED, 4));
    service.saveProcess(createProcessEntity(processName, ProcessExecutionState.FAILED, 3));

    assertThat(service.getActiveProcesses(processName)).hasSize(2);
    assertThat(service.getCompletedProcesses(processName)).hasSize(3);
    assertThat(service.getFailedProcesses(processName)).hasSize(4);

    assertThat(service.getActiveProcesses(processName))
        .isSortedAccordingTo(Comparator.comparingInt(ProcessEntity::getPriority).reversed());
    assertThat(service.getFailedProcesses(processName))
        .isSortedAccordingTo(Comparator.comparingInt(ProcessEntity::getPriority).reversed());
  }

  private static ProcessEntity createProcessEntity(
      String processName, ProcessExecutionState state, int priority) {
    return new ProcessEntity(
        UniqueStringGenerator.randomProcessId(), processName, state, 0, priority);
  }
}
