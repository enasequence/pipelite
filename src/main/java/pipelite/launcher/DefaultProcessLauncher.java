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

import com.google.common.base.Verify;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
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

import org.springframework.transaction.annotation.Transactional;

@Flogger
@Component()
@Scope("prototype")
public class DefaultProcessLauncher implements ProcessLauncher {

  @Autowired private PlatformTransactionManager transactionManager;

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService pipeliteLockService;
  private final TaskExecutor executor;
  private final ExceptionResolver resolver;

  private PipeliteProcess pipeliteProcess;
  private TaskInstance[] taskInstances;

  private boolean lock;
  private volatile boolean stop;

  public DefaultProcessLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfiguration processConfiguration,
      @Autowired TaskConfiguration taskConfiguration,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteStageService pipeliteStageService,
      @Autowired PipeliteLockService pipeliteLockService) {

    Verify.verifyNotNull(launcherConfiguration);
    Verify.verifyNotNull(processConfiguration);
    Verify.verifyNotNull(taskConfiguration);
    Verify.verifyNotNull(pipeliteProcessService);
    Verify.verifyNotNull(pipeliteStageService);
    Verify.verifyNotNull(pipeliteLockService);

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

  @Override
  @Transactional
  public boolean init(String processId) {
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
      return false;
    }

    Optional<PipeliteProcess> savedPipeliteProcess =
        pipeliteProcessService.getSavedProcess(processName, processId);
    if (!savedPipeliteProcess.isPresent()) {
      log.atWarning()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .log("Process can't be executed because it could not be retrieved");
      return false;
    }

    this.pipeliteProcess = savedPipeliteProcess.get();

    ProcessExecutionState state = pipeliteProcess.getState();

    if (state == null) {
      log.atInfo()
          .with(LogKey.PROCESS_NAME, getProcessName())
          .with(LogKey.PROCESS_ID, processId)
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
      return false;
    }

    return true;
  }

  @Override
  @Transactional
  public void execute() {
    if (stop) {
      return;
    }
    String threadName = Thread.currentThread().getName();

    String processName = getProcessName();
    String processId = pipeliteProcess.getProcessId();

    try {
      Thread.currentThread().setName(getLauncherName() + "/" + processName + "/" + processId);

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

    } finally {
      // Unlock the process.
      unlockProcess(processId);

      Thread.currentThread().setName(threadName);
    }
  }

  private boolean lockProcess(String processId) {
    log.atInfo()
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to lock process");

    if (pipeliteLockService.lockProcess(getLauncherName(), getProcessName(), processId)) {
      lock = true;
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
    if (!lock) {
      return;
    }
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
      lock = false;
    }
  }

  @Override
  @Transactional
  public void close() {
    if (pipeliteProcess != null) {
      unlockProcess(pipeliteProcess.getProcessId());
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
        pipeliteStage = Optional.of(PipeliteStage.newExecution(processId, processName, stageName));

        pipeliteStageService.saveStage(pipeliteStage.get());
      }

      taskInstances[i] =
          new TaskInstance(pipeliteProcess, pipeliteStage.get(), taskConfiguration, stage);
    }

    return taskInstances;
  }

  private void executeTasks() {
    TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
    transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

    for (TaskInstance taskInstance : taskInstances) {
      if (!transactionTemplate.execute(status -> executeTask(taskInstance))) {
        break;
      }
    }
  }

  private boolean executeTask(TaskInstance taskInstance) {
    if (stop) {
      return false; // Do not continue execution.
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
      return true; // Continue execution.
    }

    // Do not execute failed tasks or any tasks that depend on it.

    if (TaskExecutionState.FAILED == taskInstance.evaluateTaskExecutionState()) {
      log.atInfo()
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, stageName)
          .log("Task will not be executed because it has already failed");
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

      return false; // Do not continue execution.
    }

    log.atSevere()
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, taskInstance.getPipeliteStage().getStageName())
        .log("Invalidate task dependencies");

    invalidateTaskDepedencies(taskInstances, taskInstance, false);

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

  public void stop() {
    this.stop = true;
  }
}
