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
package pipelite.executor;

import lombok.extern.flogger.Flogger;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static pipelite.log.LogKey.*;
import static pipelite.task.TaskExecutionResultExitCodeSerializer.EXIT_CODE_DEFAULT_INTERNAL_ERROR;

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
      result = TaskExecutionResult.defaultInternalError();
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
          .log("Exception when creating task executor");
      System.exit(EXIT_CODE_DEFAULT_INTERNAL_ERROR);
    }

    ResultResolver resolver = null;
    try {
      resolver = (ResultResolver) Class.forName(resolverName).newInstance();
    } catch (Exception ex) {
      log.atSevere()
          .with(PROCESS_NAME, processName)
          .with(PROCESS_ID, processId)
          .with(TASK_NAME, taskName)
          .with(TASK_EXECUTOR_CLASS_NAME, executorName)
          .with(TASK_RESULT_RESOLVER_CLASS_NAME, resolverName)
          .withCause(ex)
          .log("Exception when creating result resolver");
      System.exit(EXIT_CODE_DEFAULT_INTERNAL_ERROR);
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
          .log("Exception when preparing to call internal task executor");
      System.exit(EXIT_CODE_DEFAULT_INTERNAL_ERROR);
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
          .log("Exception when calling internal task executor");

      System.exit(EXIT_CODE_DEFAULT_INTERNAL_ERROR);
    }
  }

  public static String getInternalTaskExecutorExecutable() {
    return Paths.get(System.getProperty("java.home"), "bin", "java").toString();
  }

  public static List<String> getInternalTaskExecutorArgs(TaskInstance taskInstance) {
    List<String> args = new ArrayList<>();

    args.addAll(taskInstance.getTaskParameters().getEnvAsJavaSystemPropertyOptions());

    Integer memory = taskInstance.getTaskParameters().getMemory();

    if (memory != null && memory > 0) {
      args.add(String.format("-Xmx%dM", memory));
    }

    args.add("-cp");
    args.add(System.getProperty("java.class.path"));
    args.add(InternalTaskExecutor.class.getName());

    args.add(taskInstance.getProcessName());
    args.add(taskInstance.getProcessId());
    args.add(taskInstance.getTaskName());
    args.add(taskInstance.getExecutor().getClass().getName());
    args.add(taskInstance.getResolver().getClass().getName());

    return args;
  }

  public static TaskExecutionResult getInternalTaskExecutionResult(
      TaskInstance taskInstance, int exitCode) {
    return taskInstance.getResolver().serializer().deserialize(exitCode);
  }
}
