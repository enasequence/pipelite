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
package pipelite.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import lombok.Data;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.configuration.*;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.executor.ResumableTaskExecutor;
import pipelite.executor.SerializableTaskExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.process.ProcessInstance;
import pipelite.log.LogKey;
import pipelite.process.ProcessExecutionState;
import pipelite.service.PipeliteLockService;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.task.TaskInstance;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import static pipelite.task.TaskExecutionResultType.*;

@Flogger
@Component()
@Scope("prototype")
public class ProcessLauncher extends AbstractExecutionThreadService {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService pipeliteLockService;

  private PipeliteProcessInstance pipeliteProcessInstance;
  private List<PipeliteTaskInstance> pipeliteTaskInstances = new ArrayList<>();

  private int taskFailedCount = 0;
  private int taskSkippedCount = 0;
  private int taskCompletedCount = 0;
  private int taskRecoverCount = 0;
  private int taskRecoverFailedCount = 0;

  public ProcessLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfiguration processConfiguration,
      @Autowired TaskConfiguration taskConfiguration,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteStageService pipeliteStageService,
      @Autowired PipeliteLockService pipeliteLockService) {

    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.pipeliteStageService = pipeliteStageService;
    this.pipeliteLockService = pipeliteLockService;
  }

  @Data
  private static class PipeliteProcessInstance {
    private final ProcessInstance processInstance;
    private PipeliteProcess pipeliteProcess;

    public PipeliteProcessInstance(ProcessInstance processInstance) {
      this.processInstance = processInstance;
    }
  }

  @Value
  private static class PipeliteTaskInstance {
    private final TaskInstance taskInstance;
    private final PipeliteStage pipeliteStage;
  }

  public static class ProcessNotExecutableException extends RuntimeException {
    private final String processName;
    private final String processId;

    private static String getMessage(String reason) {
      return "Process could not be executed because it could not be " + reason;
    }

    public ProcessNotExecutableException(String reason, String processName, String processId) {
      super(getMessage(reason));
      this.processName = processName;
      this.processId = processId;
    }
  }

  public void init(ProcessInstance processInstance) {
    this.pipeliteProcessInstance = new PipeliteProcessInstance(processInstance);
  }

  @Override
  public String serviceName() {
    return getProcessName();
  }

  @Override
  protected void startUp() {

    lockProcess();

    loadProcess();

    activateProcess();

    createTasks();

    evaluateProcessState();
  }

  @Override
  public void run() {
    if (!isRunning()) {
      return;
    }

    log.atInfo()
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, getProcessId())
        .log("Executing process instance");

    executeTasks();

    saveProcess();
  }

  @Override
  protected void shutDown() {
    unlockProcess();
  }

  private void lockProcess() throws ProcessNotExecutableException {
    log.atInfo()
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, getProcessId())
        .log("Locking process instance for execution");

    if (!pipeliteLockService.lockProcess(getLauncherName(), getProcessName(), getProcessId())) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, getProcessName())
          .with(LogKey.PROCESS_ID, getProcessId())
          .log(ProcessNotExecutableException.getMessage("locked"));
      throw new ProcessNotExecutableException("locked", getProcessName(), getProcessId());
    }
  }

  private void loadProcess() throws ProcessNotExecutableException {

    Optional<PipeliteProcess> pipeliteProcess =
        pipeliteProcessService.getSavedProcess(getProcessName(), getProcessId());
    if (!pipeliteProcess.isPresent()) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, getProcessName())
          .with(LogKey.PROCESS_ID, getProcessId())
          .log(ProcessNotExecutableException.getMessage("retrieved"));
      throw new ProcessNotExecutableException("retrieved", getProcessName(), getProcessId());
    }
    pipeliteProcessInstance.setPipeliteProcess(pipeliteProcess.get());
  }

  private void activateProcess() {
    PipeliteProcess pipeliteProcess = pipeliteProcessInstance.getPipeliteProcess();

    if (pipeliteProcess.getState() == null) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, getProcessName())
          .with(LogKey.PROCESS_ID, getProcessId())
          .log("Activating process");
      pipeliteProcess.setState(ProcessExecutionState.ACTIVE);
      pipeliteProcessService.saveProcess(pipeliteProcess);
    }
  }

  private void createTasks() {
    ProcessInstance processInstance = pipeliteProcessInstance.getProcessInstance();
    List<TaskInstance> taskInstances = processInstance.getTasks();

    // TODO: check that there are no orphaned saved tasks

    for (TaskInstance taskInstance : taskInstances) {

      String processId = processInstance.getProcessId();
      String processName = processInstance.getProcessName();
      String taskName = taskInstance.getTaskName();

      Optional<PipeliteStage> pipeliteStage =
          pipeliteStageService.getSavedStage(processName, processId, taskName);

      // Create and save the task it if does not already exist.

      if (!pipeliteStage.isPresent()) {
        pipeliteStage =
            Optional.of(
                pipeliteStageService.saveStage(
                    PipeliteStage.newExecution(
                        processId, processName, taskName, taskInstance.getExecutor())));
      }

      createPipeliteTaskInstance(taskInstance, pipeliteStage.get());
    }
  }

  private void evaluateProcessState() {
    PipeliteProcess pipeliteProcess = pipeliteProcessInstance.getPipeliteProcess();

    // Get process execution state from the tasks. If it is different from the
    // process execution state then update the project execution state.

    ProcessExecutionState tasksState = evaluateProcessExecutionState();
    if (pipeliteProcess.getState() != tasksState) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, getProcessName())
          .with(LogKey.PROCESS_ID, getProcessId())
          .with(LogKey.PROCESS_STATE, pipeliteProcess.getState())
          .with(LogKey.NEW_PROCESS_STATE, tasksState)
          .log("Changing process state to match state from tasks");

      pipeliteProcess.setState(tasksState);
      pipeliteProcessService.saveProcess(pipeliteProcess);
    }

    // The process needs to be active to be executed.

    if (pipeliteProcess.getState() != ProcessExecutionState.ACTIVE) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, getProcessName())
          .with(LogKey.PROCESS_ID, getProcessId())
          .with(LogKey.PROCESS_STATE, pipeliteProcess.getState())
          .log(ProcessNotExecutableException.getMessage("active"));
      throw new ProcessNotExecutableException("active", getProcessName(), getProcessId());
    }
  }

  private void unlockProcess() {
    log.atInfo()
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, getProcessId())
        .log("Unlocking process");

    // TODO: check that the process is locked by this launcher
    if (pipeliteLockService.isProcessLocked(getProcessName(), getProcessId())) {
      pipeliteLockService.unlockProcess(getLauncherName(), getProcessName(), getProcessId());
    }
  }

  private ProcessExecutionState evaluateProcessExecutionState() {
    int successCount = 0;
    for (PipeliteTaskInstance pipeliteTaskInstance : pipeliteTaskInstances) {

      TaskExecutionResultType resultType = pipeliteTaskInstance.getPipeliteStage().getResultType();

      if (resultType == null || resultType == ACTIVE || resultType == INTERNAL_ERROR) {
        return ProcessExecutionState.ACTIVE;
      }

      Integer executionCount = pipeliteTaskInstance.getPipeliteStage().getExecutionCount();
      Integer retries = pipeliteTaskInstance.getTaskInstance().getTaskParameters().getRetries();

      if (resultType == PERMANENT_ERROR
          || (resultType == TRANSIENT_ERROR
              && executionCount != null
              && retries != null
              && executionCount >= retries)) {
        return ProcessExecutionState.FAILED;
      }

      if (resultType == SUCCESS) {
        successCount++;
      }
    }

    if (successCount == pipeliteTaskInstances.size()) {
      return ProcessExecutionState.COMPLETED;
    }

    return ProcessExecutionState.ACTIVE;
  }

  private void createPipeliteTaskInstance(TaskInstance taskInstance, PipeliteStage pipeliteStage) {
    /** Add global task parameters. */
    taskInstance.getTaskParameters().add(taskConfiguration);
    pipeliteTaskInstances.add(new PipeliteTaskInstance(taskInstance, pipeliteStage));
  }

  private void executeTasks() {
    for (PipeliteTaskInstance pipeliteTaskInstance : pipeliteTaskInstances) {
      if (!executeTask(pipeliteTaskInstance)) {
        break;
      }
    }
  }

  private void saveProcess() {

    PipeliteProcess pipeliteProcess = pipeliteProcessInstance.getPipeliteProcess();

    log.atInfo()
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, getProcessId())
        .with(LogKey.PROCESS_STATE, pipeliteProcess.getState())
        .with(LogKey.PROCESS_EXECUTION_COUNT, pipeliteProcess.getExecutionCount())
        .log("Saving process state");

    pipeliteProcess.setState(evaluateProcessExecutionState());
    pipeliteProcess.incrementExecutionCount();

    pipeliteProcessService.saveProcess(pipeliteProcess);
  }

  private boolean executeTask(PipeliteTaskInstance pipeliteTaskInstance) {
    if (!isRunning()) {
      return false;
    }

    TaskInstance taskInstance = pipeliteTaskInstance.getTaskInstance();
    PipeliteStage pipeliteStage = pipeliteTaskInstance.getPipeliteStage();
    String processName = getProcessName();
    String processId = getProcessId();
    String taskName = taskInstance.getTaskName();

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.TASK_NAME, taskName)
        .log("Executing task");

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
            SerializableTaskExecutor.deserialize(
                pipeliteStage.getExecutorName(), pipeliteStage.getExecutorData());
        if (!(executor instanceof ResumableTaskExecutor)) {
          executor = null;
        }
      } catch (Exception ex) {
        ++taskRecoverFailedCount;
        log.atSevere()
            .with(LogKey.PROCESS_NAME, processName)
            .with(LogKey.PROCESS_ID, processId)
            .with(LogKey.TASK_NAME, taskName)
            .withCause(ex)
            .log("Failed to resume task execution: %s", pipeliteStage.getExecutorName());
      }

      if (executor != null) {
        try {
          result = ((ResumableTaskExecutor) executor).resume(taskInstance);
          ++taskRecoverCount;
        } catch (Exception ex) {
          ++taskRecoverFailedCount;
          log.atSevere()
              .with(LogKey.PROCESS_NAME, processName)
              .with(LogKey.PROCESS_ID, processId)
              .with(LogKey.TASK_NAME, taskName)
              .withCause(ex)
              .log("Failed to resume task execution: %s", pipeliteStage.getExecutorName());
        }
      }
    }

    if (result == null || result.isTransientError() || result.isInternalError()) {

      // Execute the task.

      pipeliteStage.retryExecution(taskInstance.getExecutor());
      pipeliteStageService.saveStage(pipeliteStage);

      try {
        result = taskInstance.getExecutor().execute(taskInstance);
      } catch (Exception ex) {
        result = TaskExecutionResult.defaultInternalError();
        result.addExceptionAttribute(ex);
      }
    }

    pipeliteStage.endExecution(
        result,
        result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND),
        result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT),
        result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR));
    pipeliteStageService.saveStage(pipeliteStage);

    if (result.isSuccess()) {
      ++taskCompletedCount;
      log.atInfo()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.TASK_NAME, pipeliteStage.getStageName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, pipeliteStage.getResultType())
          .with(LogKey.TASK_EXECUTION_RESULT, pipeliteStage.getResult())
          .with(LogKey.TASK_EXECUTION_COUNT, pipeliteStage.getExecutionCount())
          .log("Task executed successfully.");
      invalidateTaskDepedencies(pipeliteTaskInstance, false);
      return true; // Continue process execution.
    } else {
      ++taskFailedCount;
      log.atSevere()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.TASK_NAME, pipeliteStage.getStageName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, pipeliteStage.getResultType())
          .with(LogKey.TASK_EXECUTION_RESULT, pipeliteStage.getResult())
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

  public int getTaskRecoverCount() {
    return taskRecoverCount;
  }

  public int getTaskRecoverFailedCount() {
    return taskRecoverFailedCount;
  }
}
