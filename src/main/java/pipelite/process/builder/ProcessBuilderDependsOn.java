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

import pipelite.process.Process;
import pipelite.task.Task;
import pipelite.task.TaskParameters;

public class ProcessBuilderDependsOn {

  private final ProcessBuilder processBuilder;

  public ProcessBuilderDependsOn(ProcessBuilder processBuilder) {
    this.processBuilder = processBuilder;
  }

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

  public Process build() {
    return Process.builder()
        .processName(processBuilder.processName)
        .processId(processBuilder.processId)
        .priority(processBuilder.priority)
        .tasks(processBuilder.tasks)
        .build();
  }

  private Task lastTask() {
    return processBuilder.tasks.get(processBuilder.tasks.size() - 1);
  }

  private Task firstTask() {
    return processBuilder.tasks.get(0);
  }
}
