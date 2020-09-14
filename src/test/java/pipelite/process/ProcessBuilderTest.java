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

    Process process =
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

    assertThat(process).isNotNull();
    assertThat(process.getProcessName()).isEqualTo(PROCESS_NAME);
    assertThat(process.getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(process.getPriority()).isEqualTo(9);
    assertThat(process.getTasks().get(0).getProcessName()).isEqualTo(PROCESS_NAME);
    assertThat(process.getTasks().get(1).getProcessName()).isEqualTo(PROCESS_NAME);
    assertThat(process.getTasks().get(0).getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(process.getTasks().get(1).getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(process.getTasks().get(0).getDependsOn()).isNull();
    assertThat(process.getTasks().get(1).getDependsOn().getTaskName()).isEqualTo(taskName1);
    assertThat(process.getTasks().get(2).getDependsOn().getTaskName()).isEqualTo(taskName2);
    assertThat(process.getTasks().get(3).getDependsOn().getTaskName()).isEqualTo(taskName1);
    assertThat(process.getTasks()).hasSize(4);
  }
}
