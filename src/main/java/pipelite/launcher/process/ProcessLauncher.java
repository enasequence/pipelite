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
package pipelite.launcher.process;

import static pipelite.task.TaskExecutionResultType.*;

import com.google.common.flogger.FluentLogger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.configuration.*;
import pipelite.entity.ProcessEntity;
import pipelite.entity.TaskEntity;
import pipelite.executor.PollableExecutor;
import pipelite.executor.SerializableExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.log.LogKey;
import pipelite.process.ProcessExecutionState;
import pipelite.process.Process;
import pipelite.service.ProcessService;
import pipelite.service.TaskService;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.Task;

@Flogger
@Component()
@Scope("prototype")
public class ProcessLauncher implements Runnable {

  private final LauncherConfiguration launcherConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final ProcessService processService;
  private final TaskService taskService;
  private final List<TaskAndTaskEntity> taskAndTaskEntities;
  private final DependencyResolver dependencyResolver;
  private final ExecutorService executorService;
  private final Set<String> activeTasks = ConcurrentHashMap.newKeySet();
  private final Duration taskLaunchFrequency;

  public static final Duration DEFAULT_TASK_LAUNCH_FREQUENCY = Duration.ofMinutes(1);

  private ProcessAndProcessEntity processAndProcessEntity;

  private final AtomicInteger taskFailedCount = new AtomicInteger(0);
  private final AtomicInteger taskCompletedCount = new AtomicInteger(0);

  public ProcessLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired TaskConfiguration taskConfiguration,
      @Autowired ProcessService processService,
      @Autowired TaskService taskService) {

    this.launcherConfiguration = launcherConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.processService = processService;
    this.taskService = taskService;
    this.taskAndTaskEntities = new ArrayList<>();
    this.dependencyResolver = new DependencyResolver(taskAndTaskEntities);
    this.executorService = Executors.newCachedThreadPool();

    if (launcherConfiguration.getTaskLaunchFrequency() != null) {
      this.taskLaunchFrequency = launcherConfiguration.getTaskLaunchFrequency();
    } else {
      this.taskLaunchFrequency = DEFAULT_TASK_LAUNCH_FREQUENCY;
    }
  }

  private static class ProcessAndProcessEntity {
    private final Process process;
    private final ProcessEntity processEntity;

    public ProcessAndProcessEntity(
            Process process, ProcessEntity processEntity) {
      this.process = process;
      this.processEntity = processEntity;
    }

    public Process getProcess() {
      return process;
    }

    public ProcessEntity getProcessEntity() {
      return processEntity;
    }
  }

  public static class TaskAndTaskEntity {
    private final Task task;
    private final TaskEntity taskEntity;

    public TaskAndTaskEntity(Task task, TaskEntity taskEntity) {
      this.task = task;
      this.taskEntity = taskEntity;
    }

    public Task getTask() {
      return task;
    }

    public TaskEntity getTaskEntity() {
      return taskEntity;
    }
  }

  public void init(Process process, ProcessEntity processEntity) {
    this.processAndProcessEntity = new ProcessAndProcessEntity(process, processEntity);
  }

  @Override
  public void run() {
    logContext(log.atInfo()).log("Running process launcher");
    createTasks();
    executeTasks();
    saveProcess();
  }

  // TODO: orphaned saved tasks
  private void createTasks() {
    Process process = processAndProcessEntity.getProcess();
    List<Task> tasks = process.getTasks();

    for (Task task : tasks) {
      task.getTaskParameters().add(taskConfiguration);

      Optional<TaskEntity> processEntity =
          taskService.getSavedTask(
              process.getProcessName(),
              process.getProcessId(),
              task.getTaskName());

      // Create the task in database if it does not already exist.
      if (!processEntity.isPresent()) {
        processEntity =
            Optional.of(
                taskService.saveTask(TaskEntity.createExecution(task)));
      }

      taskAndTaskEntities.add(new TaskAndTaskEntity(task, processEntity.get()));
    }
  }

  private ProcessExecutionState evaluateProcessExecutionState() {
    int successCount = 0;
    for (TaskAndTaskEntity taskAndTaskEntity : taskAndTaskEntities) {

      TaskExecutionResultType resultType = taskAndTaskEntity.getTaskEntity().getResultType();

      if (resultType == SUCCESS) {
        successCount++;
      } else if (resultType == null || resultType == ACTIVE) {
        return ProcessExecutionState.ACTIVE;
      } else {
        Integer executionCount = taskAndTaskEntity.getTaskEntity().getExecutionCount();
        Integer retries = taskAndTaskEntity.getTask().getTaskParameters().getRetries();

        if (resultType == ERROR
            && executionCount != null
            && retries != null
            && executionCount >= retries) {
          return ProcessExecutionState.FAILED;
        }
      }
    }

    if (successCount == taskAndTaskEntities.size()) {
      return ProcessExecutionState.COMPLETED;
    }

    return ProcessExecutionState.ACTIVE;
  }

  private void executeTasks() {
    while (true) {
      if (Thread.currentThread().isInterrupted()) {
        executorService.shutdownNow();
        return;
      }

      logContext(log.atFine()).log("Executing tasks");

      List<TaskAndTaskEntity> runnableTasks = dependencyResolver.getRunnableTasks();
      if (runnableTasks.isEmpty()) {
        return;
      }

      for (TaskAndTaskEntity taskAndTaskEntity : runnableTasks) {
        String taskName = taskAndTaskEntity.getTask().getTaskName();
        if (activeTasks.contains(taskName)) {
          continue;
        }

        if (taskAndTaskEntity.getTask().getDependsOn() != null) {
          String dependsOnTaskName =
              taskAndTaskEntity.getTask().getDependsOn().getTaskName();
          if (dependsOnTaskName != null && activeTasks.contains(dependsOnTaskName)) {
            continue;
          }
        }

        activeTasks.add(taskName);
        executorService.execute(
            () -> {
              try {
                executeTask(taskAndTaskEntity);
              } catch (Exception ex) {
                logContext(log.atSevere())
                    .withCause(ex)
                    .log("Unexpected exception when executing task");
              } finally {
                activeTasks.remove(taskName);
              }
            });
      }

      try {
        Thread.sleep(taskLaunchFrequency.toMillis());
      } catch (InterruptedException ex) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void saveProcess() {

    ProcessEntity processEntity = processAndProcessEntity.getProcessEntity();

    logContext(log.atInfo()).log("Saving process");

    processEntity.setState(evaluateProcessExecutionState());
    processEntity.incrementExecutionCount();

    processService.saveProcess(processEntity);
  }

  private void executeTask(TaskAndTaskEntity taskAndTaskEntity) {
    Task task = taskAndTaskEntity.getTask();
    TaskEntity taskEntity = taskAndTaskEntity.getTaskEntity();
    String taskName = task.getTaskName();

    logContext(log.atInfo(), taskName).log("Executing task");

    TaskExecutionResult result = null;
    TaskExecutor executor = null;

    // Resume task execution.

    if (taskEntity.getResultType() == ACTIVE
        && taskEntity.getExecutorName() != null
        && taskEntity.getExecutorData() != null) {
      try {
        executor =
            SerializableExecutor.deserialize(
                taskEntity.getExecutorName(), taskEntity.getExecutorData());
        if (!(executor instanceof PollableExecutor)) {
          executor = null;
        }
      } catch (Exception ex) {
        logContext(log.atSevere(), taskName)
            .withCause(ex)
            .log("Failed to resume task execution: %s", taskEntity.getExecutorName());
      }

      if (executor != null) {
        try {
          result = ((PollableExecutor) executor).poll(task);
        } catch (Exception ex) {
          logContext(log.atSevere(), taskName)
              .withCause(ex)
              .log("Failed to resume task execution: %s", taskEntity.getExecutorName());
        }
      }
    }

    if (result == null || result.isError()) {
      // Execute the task.
      taskEntity.startExecution(task);
      taskService.saveTask(taskEntity);

      try {
        result = task.getExecutor().execute(task);
      } catch (Exception ex) {
        result = TaskExecutionResult.error();
        result.addExceptionAttribute(ex);
      }
    }

    if (result.isActive() && executor instanceof PollableExecutor) {
      // Save the task executor details required for polling.
      taskService.saveTask(taskEntity);
      result = ((PollableExecutor) executor).poll(task);
    }

    taskEntity.endExecution(result);
    taskService.saveTask(taskEntity);

    if (result.isSuccess()) {
      taskCompletedCount.incrementAndGet();
      logContext(log.atInfo(), taskEntity.getTaskName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, taskEntity.getResultType())
          .with(LogKey.TASK_EXECUTION_COUNT, taskEntity.getExecutionCount())
          .log("Task executed successfully.");
      invalidateDependentTasks(taskAndTaskEntity);
    } else {
      taskFailedCount.incrementAndGet();
      logContext(log.atSevere(), taskEntity.getTaskName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, taskEntity.getResultType())
          .with(LogKey.TASK_EXECUTION_COUNT, taskEntity.getExecutionCount())
          .log("Task execution failed");
    }
  }

  private void invalidateDependentTasks(TaskAndTaskEntity from) {
    for (TaskAndTaskEntity taskAndTaskEntity : dependencyResolver.getDependentTasks(from)) {
      taskAndTaskEntity.getTaskEntity().resetExecution();
      taskService.saveTask(taskAndTaskEntity.getTaskEntity());
    }
  }

  public String getLauncherName() {
    return launcherConfiguration.getLauncherName();
  }

  public String getProcessName() {
    return processAndProcessEntity.getProcess().getProcessName();
  }

  public String getProcessId() {
    return processAndProcessEntity.getProcess().getProcessId();
  }

  public int getTaskFailedCount() {
    return taskFailedCount.get();
  }

  public int getTaskCompletedCount() {
    return taskCompletedCount.get();
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, getProcessId());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String taskName) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, getProcessId())
        .with(LogKey.TASK_NAME, taskName);
  }
}
