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
package pipelite.launcher;

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
import pipelite.configuration.ProcessConfigurationEx;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.instance.ProcessInstance;
import pipelite.log.LogKey;
import pipelite.service.PipeliteLockService;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.resolver.ExceptionResolver;
import pipelite.process.ProcessExecutionState;
import pipelite.task.result.TaskExecutionResultType;
import pipelite.task.state.TaskExecutionState;
import pipelite.task.result.TaskExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInfo;

@Flogger
@Component()
@Scope("prototype")
public class DefaultProcessLauncher extends AbstractExecutionThreadService
    implements ProcessLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfigurationEx processConfiguration;
  private final TaskConfigurationEx taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService pipeliteLockService;
  private final TaskExecutor executor;
  private final ExceptionResolver resolver;

  private PipeliteProcessInstance pipeliteProcessInstance;
  private List<PipeliteTaskInstance> pipeliteTaskInstances = new ArrayList<>();

  private int taskFailedCount = 0;
  private int taskSkippedCount = 0;
  private int taskCompletedCount = 0;

  public DefaultProcessLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfigurationEx processConfiguration,
      @Autowired TaskConfigurationEx taskConfiguration,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteStageService pipeliteStageService,
      @Autowired PipeliteLockService pipeliteLockService) {

    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.pipeliteStageService = pipeliteStageService;
    this.pipeliteLockService = pipeliteLockService;
    this.executor = processConfiguration.getExecutorFactory().createTaskExecutor(taskConfiguration);
    this.resolver = taskConfiguration.getResolver();
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

    public ProcessNotExecutableException(String message, String processName, String processId) {
      super("Process can't be executed: " + message);
      this.processName = processName;
      this.processId = processId;
    }
  }

  @Override
  public void init(ProcessInstance processInstance) {
    this.pipeliteProcessInstance = new PipeliteProcessInstance(processInstance);
  }

  @Override
  public String serviceName() {
    return getProcessName();
  }

  @Override
  protected void startUp() {
    String processName = getProcessName();
    String processId = getProcessId();

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Initialising process launcher");

    // Lock the process for execution.

    if (!lockProcess(processId)) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .log("Process can't be executed because it could not be locked");
      throw new ProcessNotExecutableException(
          "process could not be locked", processName, processId);
    }

    Optional<PipeliteProcess> savedPipeliteProcess =
        pipeliteProcessService.getSavedProcess(processName, processId);
    if (!savedPipeliteProcess.isPresent()) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .log("Process can't be executed because it could not be retrieved");
      throw new ProcessNotExecutableException(
          "process could not be retrieved", processName, processId);
    }

    PipeliteProcess pipeliteProcess = savedPipeliteProcess.get();
    pipeliteProcessInstance.setPipeliteProcess(pipeliteProcess);

    ProcessExecutionState state = pipeliteProcess.getState();

    if (state == null) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, getProcessName())
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.PROCESS_STATE, state)
          .with(LogKey.NEW_PROCESS_STATE, ProcessExecutionState.ACTIVE)
          .log("Changing process state from nothing to active");

      state = ProcessExecutionState.ACTIVE;
      pipeliteProcess.setState(state);
      pipeliteProcessService.saveProcess(pipeliteProcess);
    }

    createPipeliteTaskInstances();

    // Get process execution state from the tasks. If it is different from the
    // process execution state then update the project execution state to match.

    ProcessExecutionState tasksState = evaluateProcessExecutionState();
    if (state != tasksState) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.PROCESS_STATE, state)
          .with(LogKey.NEW_PROCESS_STATE, tasksState)
          .log("Changing process state to match state derived from tasks");

      state = tasksState;
      pipeliteProcess.setState(state);
      pipeliteProcessService.saveProcess(pipeliteProcess);
    }

    // The process needs to be active to be executed.

    if (state != ProcessExecutionState.ACTIVE) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.PROCESS_STATE, state)
          .log("Process can't be executed because process state is not active");
      throw new ProcessNotExecutableException(
          "process state is not active", processName, processId);
    }
  }

  @Override
  public void run() {
    if (!isRunning()) {
      return;
    }

    String processName = getProcessName();
    String processId = getProcessId();
    PipeliteProcess pipeliteProcess = pipeliteProcessInstance.pipeliteProcess;

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Executing process");

    // Execute tasks and save their states.
    executeTasks();

    // Update and save the process state.

    pipeliteProcess.setState(evaluateProcessExecutionState());
    pipeliteProcess.incrementExecutionCount();

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.PROCESS_STATE, pipeliteProcess.getState())
        .with(LogKey.PROCESS_EXECUTION_COUNT, pipeliteProcess.getExecutionCount())
        .log("Update process state");

    pipeliteProcessService.saveProcess(pipeliteProcess);
  }

  @Override
  protected void shutDown() {
    // Unlock the process.
    unlockProcess(getProcessId());
  }

  private boolean lockProcess(String processId) {
    log.atInfo()
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to lock process");

    if (pipeliteLockService.lockProcess(getLauncherName(), getProcessName(), processId)) {
      log.atInfo()
          .with(LogKey.PROCESS_NAME, getProcessName())
          .with(LogKey.PROCESS_ID, processId)
          .log("Locked process");
      return true;
    }
    log.atWarning()
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, processId)
        .log("Failed to lock process");
    return false;
  }

  private void unlockProcess(String processId) {
    log.atInfo()
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to unlock process");

    // TODO: check that the process is locked by this launcher
    if (pipeliteLockService.isProcessLocked(getProcessName(), processId)) {
      log.atInfo()
          .with(LogKey.PROCESS_NAME, getProcessName())
          .with(LogKey.PROCESS_ID, processId)
          .log("Unlocked process");
      pipeliteLockService.unlockProcess(getLauncherName(), getProcessName(), processId);
    }
  }

  private ProcessExecutionState evaluateProcessExecutionState() {
    for (PipeliteTaskInstance pipeliteTaskInstance : pipeliteTaskInstances) {
      switch (evaluateTaskExecutionState(pipeliteTaskInstance)) {
        case ACTIVE:
          return ProcessExecutionState.ACTIVE;
        case FAILED:
          return ProcessExecutionState.FAILED;
      }
    }
    return ProcessExecutionState.COMPLETED;
  }

  private TaskExecutionState evaluateTaskExecutionState(PipeliteTaskInstance pipeliteTaskInstance) {
    TaskInstance taskInstance = pipeliteTaskInstance.getTaskInstance();
    PipeliteStage pipeliteStage = pipeliteTaskInstance.getPipeliteStage();

    if (!pipeliteStage.getEnabled()) {
      return TaskExecutionState.COMPLETED;
    }

    TaskExecutionResultType resultType = pipeliteStage.getResultType();
    if (resultType != null) {
      switch (resultType) {
        case SUCCESS:
          return TaskExecutionState.COMPLETED;
        case PERMANENT_ERROR:
        case INTERNAL_ERROR:
          return TaskExecutionState.FAILED;
        case TRANSIENT_ERROR:
          if (pipeliteStage.getExecutionCount() >= taskInstance.getTaskParameters().getRetries()) {
            return TaskExecutionState.FAILED;
          }
        default:
          return TaskExecutionState.ACTIVE;
      }
    }
    return TaskExecutionState.ACTIVE;
  }

  private void createPipeliteTaskInstances() {
    ProcessInstance processInstance = pipeliteProcessInstance.getProcessInstance();
    List<TaskInstance> taskInstances = processInstance.getTasks();

    // TODO: check that there are no orphaned saved tasks

    for (TaskInstance taskInstance : taskInstances) {

      String processId = processInstance.getProcessId();
      String processName = processInstance.getProcessName();
      String stageName = taskInstance.getTaskName();

      Optional<PipeliteStage> pipeliteStage =
          pipeliteStageService.getSavedStage(processName, processId, stageName);

      // Create and save the task it if does not already exist.

      if (!pipeliteStage.isPresent()) {
        pipeliteStage =
            Optional.of(
                pipeliteStageService.saveStage(
                    PipeliteStage.newExecution(processId, processName, stageName)));
      }

      createPipeliteTaskInstance(taskInstance, pipeliteStage.get());
    }
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

  private boolean executeTask(PipeliteTaskInstance pipeliteTaskInstance) {
    if (!isRunning()) {
      return false;
    }

    TaskInstance taskInstance = pipeliteTaskInstance.getTaskInstance();
    PipeliteStage pipeliteStage = pipeliteTaskInstance.getPipeliteStage();
    String processName = getProcessName();
    String processId = getProcessId();
    String stageName = taskInstance.getTaskName();

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stageName)
        .log("Preparing to execute task");

    // Do not execute task if it is already completed.

    if (TaskExecutionState.COMPLETED == evaluateTaskExecutionState(pipeliteTaskInstance)) {
      log.atInfo()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, stageName)
          .log("Task will not be executed because it has already completed");
      ++taskSkippedCount;
      return true; // Continue execution.
    }

    // Do not execute failed tasks or any tasks that depend on it.

    if (TaskExecutionState.FAILED == evaluateTaskExecutionState(pipeliteTaskInstance)) {
      log.atInfo()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, stageName)
          .log("Task will not be executed because it has already failed");
      ++taskFailedCount;
      return false; // Do not continue execution;.
    }

    // Update the task state before execution.

    pipeliteStage.retryExecution();
    pipeliteStageService.saveStage(pipeliteStage);

    // Execute the task.

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, pipeliteStage.getStageName())
        .with(LogKey.TASK_EXECUTION_RESULT_TYPE, pipeliteStage.getResultType())
        .with(LogKey.TASK_EXECUTION_RESULT, pipeliteStage.getResult())
        .with(LogKey.TASK_EXECUTION_COUNT, pipeliteStage.getExecutionCount())
        .log("Executing task");

    ExecutionInfo info = executor.execute(taskInstance);

    // Translate execution result.

    TaskExecutionResult result;
    if (null != info.getThrowable()) {
      result = resolver.resolveError(info.getThrowable());
    } else {
      result = resolver.exitCodeSerializer().deserialize(info.getExitCode());
    }

    // Update the task state after execution.

    pipeliteStage.endExecution(result, info.getCommandline(), info.getStdout(), info.getStderr());

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, pipeliteStage.getStageName())
        .with(LogKey.TASK_EXECUTION_RESULT_TYPE, pipeliteStage.getResultType())
        .with(LogKey.TASK_EXECUTION_RESULT, pipeliteStage.getResult())
        .with(LogKey.TASK_EXECUTION_COUNT, pipeliteStage.getExecutionCount())
        .log("Finished task execution");

    pipeliteStageService.saveStage(pipeliteStage);

    if (result.isError()) {
      log.atSevere()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, pipeliteStage.getStageName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, pipeliteStage.getResultType())
          .with(LogKey.TASK_EXECUTION_RESULT, pipeliteStage.getResult())
          .with(LogKey.TASK_EXECUTION_COUNT, pipeliteStage.getExecutionCount())
          .log("Task execution failed");

      // Do not continue execution if task execution fails.
      ++taskFailedCount;
      return false; // Do not continue execution.
    }

    log.atSevere()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, pipeliteStage.getStageName())
        .log("Invalidate task dependencies");

    invalidateTaskDepedencies(pipeliteTaskInstance, false);

    ++taskCompletedCount;
    return true; // Continue execution.
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

    if (reset && evaluateTaskExecutionState(from) != TaskExecutionState.ACTIVE) {
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
}
