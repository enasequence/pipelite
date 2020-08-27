/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.launcher;

import pipelite.Application;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.executor.AbstractTaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.task.Task;
import pipelite.task.TaskInfo;

import java.util.ArrayList;
import java.util.List;

public class InternalTaskExecutor extends AbstractTaskExecutor {

  public InternalTaskExecutor(TaskConfigurationEx taskConfiguration) {
    super(taskConfiguration);
  }

  public ExecutionInfo execute(TaskInstance taskInstance) {
    Throwable exception = null;

    try {
      String processName = taskInstance.getProcessName();
      String processId = taskInstance.getProcessId();
      String taskName = taskInstance.getTaskName();
      Task task =
          taskInstance.getTaskFactory().createTask(new TaskInfo(processName, processId, taskName));
      task.execute(taskInstance);

    } catch (Throwable e) {
      e.printStackTrace();
      exception = e;
    } finally {
      ExecutionInfo executionInfo = new ExecutionInfo();
      executionInfo.setThrowable(exception);
      executionInfo.setExitCode(resolver.exitCodeSerializer().serialize(resolver.resolveError(exception)));
      return executionInfo;
    }
  }

  public static List<String> callInternalTaskExecutor(TaskInstance instance) {
    List<String> cmd = new ArrayList<>();

    Integer memory = instance.getTaskParameters().getMemory();

    if (memory != null && memory > 0) {
      cmd.add(String.format("-Xmx%dM", memory));
    }

    cmd.addAll(instance.getTaskParameters().getEnvAsJavaSystemPropertyOptions());


    // Call Application.
    /*
    cmd.add("-cp");
    cmd.add(System.getProperty("java.class.path"));
    cmd.add(Application.class.getName());
    */

    // Arguments.
    cmd.add(Application.TASK_MODE);
    cmd.add(instance.getProcessName());
    cmd.add(instance.getProcessId());
    cmd.add(instance.getTaskName());

    return cmd;
  }
}
