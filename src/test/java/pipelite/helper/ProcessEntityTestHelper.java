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
package pipelite.helper;

import static org.assertj.core.api.Assertions.assertThat;

import pipelite.entity.ProcessEntity;
import pipelite.process.ProcessState;
import pipelite.service.ProcessService;

public class ProcessEntityTestHelper {
  private ProcessEntityTestHelper() {}

  public static void assertCompletedProcessEntity(
      ProcessService processService,
      String pipelineName,
      String processId,
      pipelite.helper.TestType testType) {
    ProcessEntity processEntity = processService.getSavedProcess(pipelineName, processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    if (testType == pipelite.helper.TestType.SUCCESS) {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    } else {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);
    }
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isAfterOrEqualTo(processEntity.getStartTime());
  }
}
