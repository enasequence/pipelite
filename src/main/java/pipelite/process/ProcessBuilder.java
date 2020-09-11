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

import lombok.EqualsAndHashCode;
import lombok.Value;
import pipelite.executor.TaskExecutor;
import pipelite.executor.command.LocalCommandExecutor;
import pipelite.executor.command.LocalTaskExecutor;
import pipelite.executor.command.SshCommandExecutor;
import pipelite.executor.command.SshTaskExecutor;
import pipelite.executor.lsf.LsfLocalCommandExecutor;
import pipelite.executor.lsf.LsfLocalTaskExecutor;
import pipelite.executor.lsf.LsfSshCommandExecutor;
import pipelite.executor.lsf.LsfSshTaskExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;

@Value
public class ProcessBuilder {

  private final String processName;
  private final String processId;
  private final int priority;
  private final List<TaskInstance> taskInstances = new ArrayList<>();

  @Value
  public class TaskInstanceBuilder {
    private final ProcessBuilder processBuilder;
    private final String taskName;
    private final TaskInstance dependsOn;
    private final TaskParameters taskParameters;

    public ProcessBuilder executor(TaskExecutor executor) {
      return addTaskInstance(executor);
    }

    public ProcessBuilder localCommandExecutor(String cmd) {
      return addTaskInstance(new LocalCommandExecutor(cmd));
    }

    public ProcessBuilder localTaskExecutor(TaskExecutor executor) {
      return addTaskInstance(new LocalTaskExecutor(executor));
    }

    public ProcessBuilder sshCommandExecutor(String cmd) {
      return addTaskInstance(new SshCommandExecutor(cmd));
    }

    public ProcessBuilder sshTaskExecutor(TaskExecutor executor) {
      return addTaskInstance(new SshTaskExecutor(executor));
    }

    public ProcessBuilder lsfLocalCommandExecutor(String cmd) {
      return addTaskInstance(new LsfLocalCommandExecutor(cmd));
    }

    public ProcessBuilder lsfLocalTaskExecutor(TaskExecutor executor) {
      return addTaskInstance(new LsfLocalTaskExecutor(executor));
    }

    public ProcessBuilder lsfSshCommandExecutor(String cmd) {
      return addTaskInstance(new LsfSshCommandExecutor(cmd));
    }

    public ProcessBuilder lsfSshTaskExecutor(TaskExecutor executor) {
      return addTaskInstance(new LsfSshTaskExecutor(executor));
    }

    private ProcessBuilder addTaskInstance(TaskExecutor executor) {
      taskInstances.add(
          TaskInstance.builder()
              .processName(processName)
              .processId(processId)
              .taskName(taskName)
              .executor(executor)
              .taskParameters(taskParameters)
              .build());
      return processBuilder;
    }
  };

  public TaskInstanceBuilder task(String taskName) {
    return new TaskInstanceBuilder(this, taskName, null, TaskParameters.builder().build());
  }

  public TaskInstanceBuilder task(String taskName, TaskParameters taskParameters) {
    return new TaskInstanceBuilder(this, taskName, null, taskParameters);
  }

  public TaskInstanceBuilder taskDependsOnPrevious(String taskName) {
    return new TaskInstanceBuilder(this, taskName, lastTask(), TaskParameters.builder().build());
  }

  public TaskInstanceBuilder taskDependsOnPrevious(
      String taskName, TaskExecutor executor, TaskParameters taskParameters) {
    return new TaskInstanceBuilder(this, taskName, lastTask(), taskParameters);
  }

  public ProcessInstance build() {
    return ProcessInstance.builder()
        .processName(processName)
        .processId(processId)
        .priority(priority)
        .tasks(taskInstances)
        .build();
  }

  private TaskInstance lastTask() {
    return taskInstances.get(taskInstances.size() - 1);
  }
}
