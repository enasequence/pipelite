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

import static pipelite.log.LogKey.*;
import static pipelite.task.TaskExecutionResultExitCode.EXIT_CODE_ERROR;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import pipelite.executor.runner.CommandRunnerUtils;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCode;
import pipelite.task.Task;
import pipelite.task.TaskParameters;

@Flogger
public class InternalExecutor implements TaskExecutor {

  @Override
  public TaskExecutionResult execute(Task task) {

    TaskExecutionResult result;

    try {
      TaskExecutor taskExecutor = task.getExecutor();
      result = taskExecutor.execute(task);
    } catch (Exception ex) {
      result = TaskExecutionResult.error();
      result.addExceptionAttribute(ex);
    }
    return result;
  }

  public static void main(String[] args) {
    String processName = args[0];
    String processId = args[1];
    String taskName = args[2];
    String executorName = args[3];

    log.atInfo()
        .with(PROCESS_NAME, processName)
        .with(PROCESS_ID, processId)
        .with(TASK_NAME, taskName)
        .with(TASK_EXECUTOR_CLASS_NAME, executorName)
        .log("System call to internal task executor");

    TaskExecutor executor = null;
    try {
      executor = (TaskExecutor) Class.forName(executorName).getDeclaredConstructor().newInstance();
    } catch (Exception ex) {
      log.atSevere()
          .with(PROCESS_NAME, processName)
          .with(PROCESS_ID, processId)
          .with(TASK_NAME, taskName)
          .with(TASK_EXECUTOR_CLASS_NAME, executorName)
          .withCause(ex)
          .log("Exception when creating task executor");
      System.exit(EXIT_CODE_ERROR);
    }

    Task task = null;

    InternalExecutor internalExecutor = null;

    try {
      internalExecutor = new InternalExecutor();

      // Task configuration is not available when a task is being executed using internal
      // task executor through a system call.

      task =
          Task.builder()
              .processName(processName)
              .processId(processId)
              .taskName(taskName)
              .executor(executor)
              .taskParameters(TaskParameters.builder().build())
              .build();
    } catch (Exception ex) {
      log.atSevere()
          .with(PROCESS_NAME, processName)
          .with(PROCESS_ID, processId)
          .with(TASK_NAME, taskName)
          .with(TASK_EXECUTOR_CLASS_NAME, executorName)
          .withCause(ex)
          .log("Exception when preparing to call internal task executor");
      System.exit(EXIT_CODE_ERROR);
    }

    try {
      TaskExecutionResult result = internalExecutor.execute(task);
      int exitCode = TaskExecutionResultExitCode.serialize(result);

      log.atInfo()
          .with(PROCESS_NAME, processName)
          .with(PROCESS_ID, processId)
          .with(TASK_NAME, taskName)
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
          .withCause(ex)
          .log("Exception when calling internal task executor");

      System.exit(EXIT_CODE_ERROR);
    }
  }

  public static String getCmd(Task task, TaskExecutor taskExecutor) {
    List<String> args = new ArrayList<>();

    args.addAll(task.getTaskParameters().getEnvAsJavaSystemPropertyOptions());

    Integer memory = task.getTaskParameters().getMemory();

    if (memory != null && memory > 0) {
      args.add(String.format("-Xmx%dM", memory));
    }

    args.add("-cp");
    args.add(System.getProperty("java.class.path"));
    args.add(CommandRunnerUtils.quoteArgument(InternalExecutor.class.getName()));

    args.add(CommandRunnerUtils.quoteArgument(task.getProcessName()));
    args.add(CommandRunnerUtils.quoteArgument(task.getProcessId()));
    args.add(CommandRunnerUtils.quoteArgument(task.getTaskName()));
    args.add(CommandRunnerUtils.quoteArgument(taskExecutor.getClass().getName()));

    return Paths.get(System.getProperty("java.home"), "bin", "java").toString()
        + " "
        + args.stream().collect(Collectors.joining(" "));
  }
}
