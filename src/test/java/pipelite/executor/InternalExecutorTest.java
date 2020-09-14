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
package pipelite.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.task.TaskExecutionResult;
import pipelite.task.Task;
import pipelite.task.TaskParameters;

public class InternalExecutorTest {

  @Test
  public void test() {

    String processName = UniqueStringGenerator.randomProcessName();
    String processId = UniqueStringGenerator.randomProcessId();
    String taskName = UniqueStringGenerator.randomTaskName();

    AtomicInteger taskExecutionCount = new AtomicInteger();

    InternalExecutor internalExecutor = new InternalExecutor();

    TaskExecutor taskExecutor =
        taskInstance -> {
          taskExecutionCount.getAndIncrement();
          return TaskExecutionResult.success();
        };

    TaskParameters taskParameters = TaskParameters.builder().build();

    Task task =
        Task.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(taskExecutor)
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = internalExecutor.execute(task);
    assertThat(result).isEqualTo(TaskExecutionResult.success());
    assertThat(taskExecutionCount.get()).isEqualTo(1);
  }
}
