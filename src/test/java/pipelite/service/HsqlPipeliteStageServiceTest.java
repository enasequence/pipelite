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
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.FullTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteStage;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test"})
class HsqlPipeliteStageServiceTest {

  @Autowired PipeliteStageService service;

  @Test
  @Transactional
  @Rollback
  public void testCrud() {

    String processId = UniqueStringGenerator.randomProcessId();
    String processName = UniqueStringGenerator.randomProcessName();
    String taskName = UniqueStringGenerator.randomTaskName();

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .build();

    PipeliteStage stage = PipeliteStage.createExecution(taskInstance);

    service.saveStage(stage);

    assertThat(service.getSavedStage(processName, processId, taskName).get()).isEqualTo(stage);

    stage.endExecution(TaskExecutionResult.success());

    service.saveStage(stage);

    assertThat(service.getSavedStage(processName, processId, taskName).get()).isEqualTo(stage);

    service.delete(stage);

    assertThat(service.getSavedStage(processName, processId, taskName).isPresent()).isFalse();
  }
}
