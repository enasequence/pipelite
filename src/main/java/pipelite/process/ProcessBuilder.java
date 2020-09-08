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

import java.util.ArrayList;
import java.util.List;
import lombok.Value;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;

@Value
public class ProcessBuilder {

  private final String processName;
  private final String processId;
  private final int priority;
  private final List<TaskInstance> taskInstances = new ArrayList<>();

  public ProcessBuilder task(String taskName, TaskExecutor executor) {
    return task(taskName, executor, TaskParameters.builder().build());
  }

  public ProcessBuilder task(
      String taskName, TaskExecutor executor, TaskParameters taskParameters) {

    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(executor)
            .taskParameters(taskParameters)
            .build());
    return this;
  }

  public ProcessBuilder taskDependsOnPrevious(String taskName, TaskExecutor executor) {
    return taskDependsOnPrevious(taskName, executor, TaskParameters.builder().build());
  }

  public ProcessBuilder taskDependsOnPrevious(
      String taskName, TaskExecutor executor, TaskParameters taskParameters) {

    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(executor)
            .taskParameters(taskParameters)
            .dependsOn(taskInstances.get(taskInstances.size() - 1))
            .build());
    return this;
  }

  public ProcessInstance build() {
    return ProcessInstance.builder()
        .processName(processName)
        .processId(processId)
        .priority(priority)
        .tasks(taskInstances)
        .build();
  }
}
