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

import java.util.Optional;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.log.LogKey;
import pipelite.service.PipeliteLockService;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.resolver.ExceptionResolver;
import pipelite.process.ProcessExecutionState;
import pipelite.task.state.TaskExecutionState;
import pipelite.task.result.TaskExecutionResult;
import pipelite.stage.Stage;
import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInfo;

@Flogger
@Component()
@Scope("prototype")
public class DefaultProcessLauncher extends AbstractExecutionThreadService
    implements ProcessLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService pipeliteLockService;
  private final TaskExecutor executor;
  private final ExceptionResolver resolver;

  private String processId;
  private PipeliteProcess pipeliteProcess;
  private TaskInstance[] taskInstances;

  private int taskFailedCount = 0;
  private int taskSkippedCount = 0;
  private int taskCompletedCount = 0;

  public DefaultProcessLauncher(
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
    this.executor =
        processConfiguration
            .createExecutorFactory()
            .create(processConfiguration, taskConfiguration);
    this.resolver = processConfiguration.createResolver();
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
  public void init(String processId) {
    this.processId = processId;
  }

  @Override
  public String serviceName() {
    return getProcessName();
  }

  @Override
  protected void startUp() {
    String processName = getProcessName();

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

    this.pipeliteProcess = savedPipeliteProcess.get();

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

    // Get a list of process tasks. If task A depends on task B then
    // task A will appear before task B.

    taskInstances = getTaskInstances();

    // Get process execution state from the tasks. If it is different from the
    // process execution state then update the project execution state to match.

    ProcessExecutionState tasksState = getProcessExecutionState(taskInstances);
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
    String processId = pipeliteProcess.getProcessId();

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Executing process");

    // Execute tasks and save their states.
    executeTasks();

    // Update and save the process state.

    pipeliteProcess.setState(getProcessExecutionState(taskInstances));
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
    unlockProcess(processId);
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

  private ProcessExecutionState getProcessExecutionState(TaskInstance[] taskInstances) {
    for (TaskInstance taskInstance : taskInstances) {
      switch (taskInstance.evaluateTaskExecutionState()) {
        case ACTIVE:
          return ProcessExecutionState.ACTIVE;
        case FAILED:
          return ProcessExecutionState.FAILED;
      }
    }
    return ProcessExecutionState.COMPLETED;
  }

  private TaskInstance[] getTaskInstances() {
    Stage[] stages = getStages();
    TaskInstance[] taskInstances = new TaskInstance[stages.length];

    for (int i = 0; i < taskInstances.length; ++i) {
      Stage stage = stages[i];
      String processId = pipeliteProcess.getProcessId();
      String processName = pipeliteProcess.getProcessName();
      String stageName = stage.getStageName();

      Optional<PipeliteStage> pipeliteStage =
          pipeliteStageService.getSavedStage(processName, processId, stageName);

      // Create and save the task it if does not already exist.

      if (!pipeliteStage.isPresent()) {
        pipeliteStage =
            Optional.of(
                pipeliteStageService.saveStage(
                    PipeliteStage.newExecution(processId, processName, stageName)));
      }

      taskInstances[i] =
          new TaskInstance(pipeliteProcess, pipeliteStage.get(), taskConfiguration, stage);
    }

    return taskInstances;
  }

  private void executeTasks() {
    for (TaskInstance taskInstance : taskInstances) {
      if (!executeTask(taskInstance)) {
        break;
      }
    }
  }

  private boolean executeTask(TaskInstance taskInstance) {
    if (!isRunning()) {
      return false;
    }

    String processName = getProcessName();
    String processId = pipeliteProcess.getProcessId();
    String stageName = taskInstance.getPipeliteStage().getStageName();

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stageName)
        .log("Preparing to execute task");

    // Do not execute task if it is already completed.

    if (TaskExecutionState.COMPLETED == taskInstance.evaluateTaskExecutionState()) {
      log.atInfo()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, stageName)
          .log("Task will not be executed because it has already completed");
      ++taskSkippedCount;
      return true; // Continue execution.
    }

    // Do not execute failed tasks or any tasks that depend on it.

    if (TaskExecutionState.FAILED == taskInstance.evaluateTaskExecutionState()) {
      log.atInfo()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, stageName)
          .log("Task will not be executed because it has already failed");
      ++taskFailedCount;
      return false; // Do not continue execution;.
    }

    // Update the task state before execution.

    taskInstance.getPipeliteStage().retryExecution();
    pipeliteStageService.saveStage(taskInstance.getPipeliteStage());

    // Execute the task.

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, taskInstance.getPipeliteStage().getStageName())
        .with(LogKey.TASK_EXECUTION_RESULT_TYPE, taskInstance.getPipeliteStage().getResultType())
        .with(LogKey.TASK_EXECUTION_RESULT, taskInstance.getPipeliteStage().getResult())
        .with(LogKey.TASK_EXECUTION_COUNT, taskInstance.getPipeliteStage().getExecutionCount())
        .log("Executing task");

    executor.execute(taskInstance);

    ExecutionInfo info = executor.get_info();

    // Translate execution result.

    TaskExecutionResult result;
    if (null != info.getThrowable()) {
      result = resolver.resolveError(info.getThrowable());
    } else {
      result = resolver.exitCodeSerializer().deserialize(info.getExitCode());
    }

    // Update the task state after execution.

    taskInstance
        .getPipeliteStage()
        .endExecution(result, info.getCommandline(), info.getStdout(), info.getStderr());

    log.atInfo()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, taskInstance.getPipeliteStage().getStageName())
        .with(LogKey.TASK_EXECUTION_RESULT_TYPE, taskInstance.getPipeliteStage().getResultType())
        .with(LogKey.TASK_EXECUTION_RESULT, taskInstance.getPipeliteStage().getResult())
        .with(LogKey.TASK_EXECUTION_COUNT, taskInstance.getPipeliteStage().getExecutionCount())
        .log("Finished task execution");

    pipeliteStageService.saveStage(taskInstance.getPipeliteStage());

    if (result.isError()) {
      log.atSevere()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, taskInstance.getPipeliteStage().getStageName())
          .with(LogKey.TASK_EXECUTION_RESULT_TYPE, taskInstance.getPipeliteStage().getResultType())
          .with(LogKey.TASK_EXECUTION_RESULT, taskInstance.getPipeliteStage().getResult())
          .with(LogKey.TASK_EXECUTION_COUNT, taskInstance.getPipeliteStage().getExecutionCount())
          .log("Task execution failed");

      // Do not continue execution if task execution fails.
      ++taskFailedCount;
      return false; // Do not continue execution.
    }

    log.atSevere()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, taskInstance.getPipeliteStage().getStageName())
        .log("Invalidate task dependencies");

    invalidateTaskDepedencies(taskInstances, taskInstance, false);

    ++taskCompletedCount;
    return true; // Continue execution.
  }

  private void invalidateTaskDepedencies(
      TaskInstance[] taskInstances, TaskInstance resetTaskInstance, boolean reset) {
    for (TaskInstance taskInstance : taskInstances) {
      if (taskInstance.equals(resetTaskInstance)) {
        continue;
      }

      Stage stageDependsOn = taskInstance.getStage().getDependsOn();
      if (stageDependsOn != null
          && stageDependsOn.getStageName().equals(resetTaskInstance.getStage().getStageName())) {
        invalidateTaskDepedencies(taskInstances, taskInstance, true);
      }
    }

    if (reset && resetTaskInstance.evaluateTaskExecutionState() != TaskExecutionState.ACTIVE) {
      resetTaskInstance.getPipeliteStage().resetExecution();
      pipeliteStageService.saveStage(resetTaskInstance.getPipeliteStage());
    }
  }

  public String getLauncherName() {
    return launcherConfiguration.getLauncherName();
  }

  public String getProcessName() {
    return processConfiguration.getProcessName();
  }

  public String getProcessId() {
    return processId;
  }

  public Stage[] getStages() {
    return processConfiguration.getStageArray();
  }

  public ProcessExecutionState getState() {
    if (pipeliteProcess != null) {
      return pipeliteProcess.getState();
    } else {
      return null;
    }
  }

  public Integer getExecutionCount() {
    if (pipeliteProcess != null) {
      return pipeliteProcess.getExecutionCount();
    } else {
      return null;
    }
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
