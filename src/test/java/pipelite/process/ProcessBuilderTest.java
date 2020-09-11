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
package pipelite.process;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.process.builder.ProcessBuilder;

public class ProcessBuilderTest {

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();
  private static final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  @Test
  public void test() {
    String taskName1 = UniqueStringGenerator.randomTaskName();
    String taskName2 = UniqueStringGenerator.randomTaskName();
    String taskName3 = UniqueStringGenerator.randomTaskName();
    String taskName4 = UniqueStringGenerator.randomTaskName();

    ProcessInstance processInstance =
        new ProcessBuilder(PROCESS_NAME, PROCESS_ID, 9)
            .task(taskName1)
            .executor(new SuccessTaskExecutor())
            .taskDependsOnPrevious(taskName2)
            .executor(new SuccessTaskExecutor())
            .taskDependsOnPrevious(taskName3)
            .executor(new SuccessTaskExecutor())
            .taskDependsOnFirst(taskName4)
            .executor(new SuccessTaskExecutor())
            .build();

    assertThat(processInstance).isNotNull();
    assertThat(processInstance.getProcessName()).isEqualTo(PROCESS_NAME);
    assertThat(processInstance.getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(processInstance.getPriority()).isEqualTo(9);
    assertThat(processInstance.getTasks().get(0).getProcessName()).isEqualTo(PROCESS_NAME);
    assertThat(processInstance.getTasks().get(1).getProcessName()).isEqualTo(PROCESS_NAME);
    assertThat(processInstance.getTasks().get(0).getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(processInstance.getTasks().get(1).getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(processInstance.getTasks().get(0).getDependsOn()).isNull();
    assertThat(processInstance.getTasks().get(1).getDependsOn().getTaskName()).isEqualTo(taskName1);
    assertThat(processInstance.getTasks().get(2).getDependsOn().getTaskName()).isEqualTo(taskName2);
    assertThat(processInstance.getTasks().get(3).getDependsOn().getTaskName()).isEqualTo(taskName1);
    assertThat(processInstance.getTasks()).hasSize(4);
  }
}
