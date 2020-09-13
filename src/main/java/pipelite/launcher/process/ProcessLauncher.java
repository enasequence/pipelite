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
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.executor.PollableExecutor;
import pipelite.executor.SerializableExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.log.LogKey;
import pipelite.process.ProcessExecutionState;
import pipelite.process.ProcessInstance;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.TaskInstance;

@Flogger
@Component()
@Scope("prototype")
public class ProcessLauncher implements Runnable {

  private final LauncherConfiguration launcherConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final List<PipeliteTaskInstance> pipeliteTaskInstances;
  private final DependencyResolver dependencyResolver;
  private final ExecutorService executorService;
  private final Set<String> activeTasks = ConcurrentHashMap.newKeySet();
  private final Duration taskLaunchFrequency;

  public static final Duration DEFAULT_TASK_LAUNCH_FREQUENCY = Duration.ofMinutes(1);

  private PipeliteProcessInstance pipeliteProcessInstance;

  private final AtomicInteger taskFailedCount = new AtomicInteger(0);
  private final AtomicInteger taskCompletedCount = new AtomicInteger(0);

  public ProcessLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired TaskConfiguration taskConfiguration,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteStageService pipeliteStageService) {

    this.launcherConfiguration = launcherConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.pipeliteStageService = pipeliteStageService;
    this.pipeliteTaskInstances = new ArrayList<>();
    this.dependencyResolver = new DependencyResolver(pipeliteTaskInstances);
    this.executorService = Executors.newCachedThreadPool();

    if (launcherConfiguration.getTaskLaunchFrequency() != null) {
      this.taskLaunchFrequency = launcherConfiguration.getTaskLaunchFrequency();
    } else {
      this.taskLaunchFrequency = DEFAULT_TASK_LAUNCH_FREQUENCY;
    }
  }

  private static class PipeliteProcessInstance {
    private final ProcessInstance processInstance;
    private final PipeliteProcess pipeliteProcess;

    public PipeliteProcessInstance(
        ProcessInstance processInstance, PipeliteProcess pipeliteProcess) {
      this.processInstance = processInstance;
      this.pipeliteProcess = pipeliteProcess;
    }

    public ProcessInstance getProcessInstance() {
      return processInstance;
    }

    public PipeliteProcess getPipeliteProcess() {
      return pipeliteProcess;
    }
  }

  public static class PipeliteTaskInstance {
    private final TaskInstance taskInstance;
    private final PipeliteStage pipeliteStage;

    public PipeliteTaskInstance(TaskInstance taskInstance, PipeliteStage pipeliteStage) {
      this.taskInstance = taskInstance;
      this.pipeliteStage = pipeliteStage;
    }

    public TaskInstance getTaskInstance() {
      return taskInstance;
    }

    public PipeliteStage getPipeliteStage() {
      return pipeliteStage;
    }
  }

  public void init(ProcessInstance processInstance, PipeliteProcess pipeliteProcess) {
    this.pipeliteProcessInstance = new PipeliteProcessInstance(processInstance, pipeliteProcess);
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
    ProcessInstance processInstance = pipeliteProcessInstance.getProcessInstance();
    List<TaskInstance> taskInstances = processInstance.getTasks();

    for (TaskInstance taskInstance : taskInstances) {
      taskInstance.getTaskParameters().add(taskConfiguration);

      Optional<PipeliteStage> pipeliteStage =
          pipeliteStageService.getSavedStage(
              processInstance.getProcessName(),
              processInstance.getProcessId(),
              taskInstance.getTaskName());

      // Create the task in database if it does not already exist.
      if (!pipeliteStage.isPresent()) {
        pipeliteStage =
            Optional.of(
                pipeliteStageService.saveStage(PipeliteStage.createExecution(taskInstance)));
      }

      pipeliteTaskInstances.add(new PipeliteTaskInstance(taskInstance, pipeliteStage.get()));
    }
  }

  private ProcessExecutionState evaluateProcessExecutionState() {
    int successCount = 0;
    for (PipeliteTaskInstance pipeliteTaskInstance : pipeliteTaskInstances) {

      TaskExecutionResultType resultType = pipeliteTaskInstance.getPipeliteStage().getResultType();

      if (resultType == SUCCESS) {
        successCount++;
      } else if (resultType == null || resultType == ACTIVE) {
        return ProcessExecutionState.ACTIVE;
      } else {
        Integer executionCount = pipeliteTaskInstance.getPipeliteStage().getExecutionCount();
        Integer retries = pipeliteTaskInstance.getTaskInstance().getTaskParameters().getRetries();

        if (resultType == ERROR
            && executionCount != null
            && retries != null
            && executionCount >= retries) {
          return ProcessExecutionState.FAILED;
        }
      }
    }

    if (successCount == pipeliteTaskInstances.size()) {
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

      List<PipeliteTaskInstance> runnableTasks = dependencyResolver.getRunnableTasks();
      if (runnableTasks.isEmpty()) {
        return;
      }

      for (PipeliteTaskInstance pipeliteTaskInstance : runnableTasks) {
        String taskName = pipeliteTaskInstance.getTaskInstance().getTaskName();
        if (activeTasks.contains(taskName)) {
          continue;
        }

        if (pipeliteTaskInstance.getTaskInstance().getDependsOn() != null) {
          String dependsOnTaskName =
              pipeliteTaskInstance.getTaskInstance().getDependsOn().getTaskName();
          if (dependsOnTaskName != null && activeTasks.contains(dependsOnTaskName)) {
            continue;
          }
        }

        activeTasks.add(taskName);
        executorService.execute(
            () -> {
              try {
                executeTask(pipeliteTaskInstance);
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

    PipeliteProcess pipeliteProcess = pipeliteProcessInstance.getPipeliteProcess();

    logContext(log.atInfo()).log("Saving process");

    pipeliteProcess.setState(evaluateProcessExecutionState());
    pipeliteProcess.incrementExecutionCount();

    pipeliteProcessService.saveProcess(pipeliteProcess);
  }

  private void executeTask(PipeliteTaskInstance pipeliteTaskInstance) {
    TaskInstance taskInstance = pipeliteTaskInstance.getTaskInstance();
    PipeliteStage pipeliteStage = pipeliteTaskInstance.getPipeliteStage();
    String taskName = taskInstance.getTaskName();

    logContext(log.atInfo(), taskName).log("Executing task");

    TaskExecutionResult result = null;
    TaskExecutor executor = null;

    // Resume task execution.

    if (pipeliteStage.getResultType() == ACTIVE
        && pipeliteStage.getExecutorName() != null
        && pipeliteStage.getExecutorData() != null) {
      try {
        executor =
            SerializableExecutor.deserialize(
                pipeliteStage.getExecutorName(), pipeliteStage.getExecutorData());
        if (!(executor instanceof PollableExecutor)) {
          executor = null;
        }
      } catch (Exception ex) {
        logContext(log.atSevere(), taskName)
            .withCause(ex)
            .log("Failed to resume task execution: %s", pipeliteStage.getExecutorName());
      }

      if (executor != null) {
        try {
          result = ((PollableExecutor) executor).poll(taskInstance);
        } catch (Exception ex) {
          logContext(log.atSevere(), taskName)
              .withCause(ex)
              .log("Failed to resume task execution: %s", pipeliteStage.getExecutorName());
        }
      }
    }

    if (result == null || result.isError()) {
      // Execute the task.
      pipeliteStage.startExecution(taskInstance);
      pipeliteStageService.saveStage(pipeliteStage);

      try {
        result = taskInstance.getExecutor().execute(taskInstance);
      } catch (Exception ex) {
        result = TaskExecutionResult.error();
        result.addExceptionAttribute(ex);
      }
    }

    if (result.isActive() && executor instanceof PollableExecutor) {
      // Save the task executor details required for polling.
      pipeliteStageService.saveStage(pipeliteStage);
      result = ((PollableExecutor) executor).poll(taskInstance);
    }

    pipeliteStage.endExecution(result);
    pipeliteStageService.saveStage(pipeliteStage);

    if (result.isSuccess()) {
      taskCompletedCount.incrementAndGet();
      logContext(log.atInfo(), pipeliteStage.getStageName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, pipeliteStage.getResultType())
          .with(LogKey.TASK_EXECUTION_COUNT, pipeliteStage.getExecutionCount())
          .log("Task executed successfully.");
      invalidateDependentTasks(pipeliteTaskInstance);
    } else {
      taskFailedCount.incrementAndGet();
      logContext(log.atSevere(), pipeliteStage.getStageName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, pipeliteStage.getResultType())
          .with(LogKey.TASK_EXECUTION_COUNT, pipeliteStage.getExecutionCount())
          .log("Task execution failed");
    }
  }

  private void invalidateDependentTasks(PipeliteTaskInstance from) {
    for (PipeliteTaskInstance pipeliteTaskInstance : dependencyResolver.getDependentTasks(from)) {
      pipeliteTaskInstance.getPipeliteStage().resetExecution();
      pipeliteStageService.saveStage(pipeliteTaskInstance.getPipeliteStage());
    }
  }

  public String getLauncherName() {
    return launcherConfiguration.getLauncherName();
  }

  public String getProcessName() {
    return pipeliteProcessInstance.getProcessInstance().getProcessName();
  }

  public String getProcessId() {
    return pipeliteProcessInstance.getProcessInstance().getProcessId();
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
