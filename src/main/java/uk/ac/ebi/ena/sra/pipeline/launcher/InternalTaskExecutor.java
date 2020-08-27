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
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.task.result.TaskExecutionResult;

import java.util.ArrayList;
import java.util.List;

public class InternalTaskExecutor implements TaskExecutor {

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {

    TaskExecutionResult result;

    try {
      TaskExecutor taskExecutor = taskInstance.getTaskExecutorFactory().createTaskExecutor();

      try {
        taskExecutor.execute(taskInstance);
        result = TaskExecutionResult.success();
      } catch (Exception ex) {
        result = taskInstance.getTaskParameters().getResolver().resolve(ex);
        result.addExceptionAttribute(ex);
      }
    } catch (Exception ex) {
      result = TaskExecutionResult.internalError();
      result.addExceptionAttribute(ex);
    }
    return result;
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
