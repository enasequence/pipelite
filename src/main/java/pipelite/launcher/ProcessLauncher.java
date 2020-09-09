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
package pipelite.launcher;

import static pipelite.task.TaskExecutionResultType.*;

import com.google.common.flogger.FluentLogger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Data;
import lombok.Value;
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
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;

  private PipeliteProcessInstance pipeliteProcessInstance;
  private List<PipeliteTaskInstance> pipeliteTaskInstances = new ArrayList<>();

  private int taskFailedCount = 0;
  private int taskSkippedCount = 0;
  private int taskCompletedCount = 0;

  public ProcessLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfiguration processConfiguration,
      @Autowired TaskConfiguration taskConfiguration,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteStageService pipeliteStageService) {

    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.pipeliteStageService = pipeliteStageService;
  }

  @Data
  private static class PipeliteProcessInstance {
    private final ProcessInstance processInstance;
    private final PipeliteProcess pipeliteProcess;
  }

  @Value
  private static class PipeliteTaskInstance {
    private final TaskInstance taskInstance;
    private final PipeliteStage pipeliteStage;
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
    for (PipeliteTaskInstance pipeliteTaskInstance : pipeliteTaskInstances) {
      if (Thread.currentThread().isInterrupted()) {
        break;
      }
      if (!executeTask(pipeliteTaskInstance)) {
        break;
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

  private boolean executeTask(PipeliteTaskInstance pipeliteTaskInstance) {
    TaskInstance taskInstance = pipeliteTaskInstance.getTaskInstance();
    PipeliteStage pipeliteStage = pipeliteTaskInstance.getPipeliteStage();
    String taskName = taskInstance.getTaskName();

    logContext(log.atInfo(), taskName).log("Executing task");

    if (pipeliteStage.getResultType() == SUCCESS) {
      // Continue executing the process.
      ++taskSkippedCount;
      return true;
    }

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
      ++taskCompletedCount;
      logContext(log.atInfo(), pipeliteStage.getStageName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, pipeliteStage.getResultType())
          .with(LogKey.TASK_EXECUTION_COUNT, pipeliteStage.getExecutionCount())
          .log("Task executed successfully.");
      invalidateTaskDepedencies(pipeliteTaskInstance, false);
      return true; // Continue process execution.
    } else {
      ++taskFailedCount;
      logContext(log.atSevere(), pipeliteStage.getStageName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, pipeliteStage.getResultType())
          .with(LogKey.TASK_EXECUTION_COUNT, pipeliteStage.getExecutionCount())
          .log("Task execution failed");
      return false; // Do not continue executing the process.
    }
  }

  private void invalidateTaskDepedencies(PipeliteTaskInstance from, boolean reset) {
    for (PipeliteTaskInstance task : pipeliteTaskInstances) {
      if (task.getTaskInstance().equals(from)) {
        continue;
      }

      TaskInstance dependsOn = task.getTaskInstance().getDependsOn();
      if (dependsOn != null && dependsOn.equals(from.getTaskInstance().getTaskName())) {
        invalidateTaskDepedencies(task, true);
      }
    }

    if (reset) {
      from.getPipeliteStage().resetExecution();
      pipeliteStageService.saveStage(from.getPipeliteStage());
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
    return taskFailedCount;
  }

  public int getTaskSkippedCount() {
    return taskSkippedCount;
  }

  public int getTaskCompletedCount() {
    return taskCompletedCount;
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
