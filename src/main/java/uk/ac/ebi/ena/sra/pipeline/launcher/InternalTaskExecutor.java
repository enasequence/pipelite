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

import lombok.extern.flogger.Flogger;
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.resolver.TaskExecutionResultResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;

import static pipelite.log.LogKey.*;

@Flogger
public class InternalTaskExecutor implements TaskExecutor {

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {

    TaskExecutionResult result;

    try {
      TaskExecutor taskExecutor = taskInstance.getExecutor();

      try {
        result = taskExecutor.execute(taskInstance);
      } catch (Exception ex) {
        result = taskInstance.getResolver().resolve(ex);
        result.addExceptionAttribute(ex);
      }
    } catch (Exception ex) {
      result = TaskExecutionResult.internalError();
      result.addExceptionAttribute(ex);
    }
    return result;
  }

  public static void main(String[] args) {
    String processName = args[0];
    String processId = args[1];
    String taskName = args[2];
    String executorName = args[3];
    String resolverName = args[4];

    log.atInfo()
        .with(PROCESS_NAME, processName)
        .with(PROCESS_ID, processId)
        .with(TASK_NAME, taskName)
        .with(TASK_EXECUTOR_CLASS_NAME, executorName)
        .with(TASK_RESULT_RESOLVER_CLASS_NAME, resolverName)
        .log("System call to internal task executor");

    TaskExecutor executor = null;
    try {
      executor = (TaskExecutor) Class.forName(executorName).newInstance();
    } catch (Exception ex) {
      log.atSevere()
          .with(PROCESS_NAME, processName)
          .with(PROCESS_ID, processId)
          .with(TASK_NAME, taskName)
          .with(TASK_EXECUTOR_CLASS_NAME, executorName)
          .with(TASK_RESULT_RESOLVER_CLASS_NAME, resolverName)
          .withCause(ex)
          .log("Exception when creating task executor for internal task executor");
      System.exit(TaskExecutionResultExitCodeSerializer.INTERNAL_ERROR_EXIT_CODE);
    }

    TaskExecutionResultResolver resolver = null;
    try {
      resolver = (TaskExecutionResultResolver) Class.forName(resolverName).newInstance();
    } catch (Exception ex) {
      log.atSevere()
          .with(PROCESS_NAME, processName)
          .with(PROCESS_ID, processId)
          .with(TASK_NAME, taskName)
          .with(TASK_EXECUTOR_CLASS_NAME, executorName)
          .with(TASK_RESULT_RESOLVER_CLASS_NAME, resolverName)
          .withCause(ex)
          .log("Exception when creating task executor result resolver for internal task executor");
      System.exit(TaskExecutionResultExitCodeSerializer.INTERNAL_ERROR_EXIT_CODE);
    }

    TaskInstance taskInstance = null;

    InternalTaskExecutor internalTaskExecutor = null;

    try {
      internalTaskExecutor = new InternalTaskExecutor();

      // Task configuration is not available when a task is being executed using internal
      // task executor through a system call.

      taskInstance =
          TaskInstance.builder()
              .processName(processName)
              .processId(processId)
              .taskName(taskName)
              .executor(executor)
              .resolver(resolver)
              .taskParameters(TaskParameters.builder().build())
              .build();
    } catch (Exception ex) {
      log.atSevere()
          .with(PROCESS_NAME, processName)
          .with(PROCESS_ID, processId)
          .with(TASK_NAME, taskName)
          .with(TASK_EXECUTOR_CLASS_NAME, executorName)
          .with(TASK_RESULT_RESOLVER_CLASS_NAME, resolverName)
          .withCause(ex)
          .log("Exception when preparing to call the internal task executor");
      System.exit(TaskExecutionResultExitCodeSerializer.INTERNAL_ERROR_EXIT_CODE);
    }

    try {
      TaskExecutionResult result = internalTaskExecutor.execute(taskInstance);
      int exitCode = taskInstance.getResolver().serializer().serialize(result);

      log.atInfo()
          .with(PROCESS_NAME, processName)
          .with(PROCESS_ID, processId)
          .with(TASK_NAME, taskName)
          .with(TASK_EXECUTION_RESULT, result.getResult())
          .with(TASK_EXECUTION_RESULT_TYPE, result.getResultType())
          .with(EXIT_CODE, exitCode)
          .log("Internal task executor completed");

      System.exit(exitCode);

    } catch (Exception ex) {
      log.atSevere()
          .with(PROCESS_NAME, processName)
          .with(PROCESS_ID, processId)
          .with(TASK_NAME, taskName)
          .with(TASK_EXECUTOR_CLASS_NAME, executorName)
          .with(TASK_RESULT_RESOLVER_CLASS_NAME, resolverName)
          .withCause(ex)
          .log("Exception when executing internal task executor");

      System.exit(TaskExecutionResultExitCodeSerializer.INTERNAL_ERROR_EXIT_CODE);
    }
  }
}
