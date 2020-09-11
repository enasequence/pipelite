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
package pipelite.process.builder;

import lombok.Value;
import pipelite.process.ProcessInstance;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;

@Value
public class ProcessBuilderDependsOn {

  private final ProcessBuilder processBuilder;

  public TaskBuilder taskDependsOn(String taskName, String dependsOnTaskName) {
    return new TaskBuilder(
        processBuilder, taskName, dependsOnTaskName, TaskParameters.builder().build());
  }

  public TaskBuilder taskDependsOn(
      String taskName, String dependsOnTaskName, TaskParameters taskParameters) {
    return new TaskBuilder(processBuilder, taskName, dependsOnTaskName, taskParameters);
  }

  public TaskBuilder taskDependsOnPrevious(String taskName) {
    return new TaskBuilder(
        processBuilder, taskName, lastTask().getTaskName(), TaskParameters.builder().build());
  }

  public TaskBuilder taskDependsOnPrevious(String taskName, TaskParameters taskParameters) {
    return new TaskBuilder(processBuilder, taskName, lastTask().getTaskName(), taskParameters);
  }

  public TaskBuilder taskDependsOnFirst(String taskName) {
    return new TaskBuilder(
        processBuilder, taskName, firstTask().getTaskName(), TaskParameters.builder().build());
  }

  public TaskBuilder taskDependsOnFirst(String taskName, TaskParameters taskParameters) {
    return new TaskBuilder(processBuilder, taskName, firstTask().getTaskName(), taskParameters);
  }

  public ProcessInstance build() {
    return ProcessInstance.builder()
        .processName(processBuilder.processName)
        .processId(processBuilder.processId)
        .priority(processBuilder.priority)
        .tasks(processBuilder.taskInstances)
        .build();
  }

  private TaskInstance lastTask() {
    return processBuilder.taskInstances.get(processBuilder.taskInstances.size() - 1);
  }

  private TaskInstance firstTask() {
    return processBuilder.taskInstances.get(0);
  }
}
