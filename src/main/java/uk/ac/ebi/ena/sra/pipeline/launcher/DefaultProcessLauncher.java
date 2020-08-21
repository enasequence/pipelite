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

import java.util.Optional;

import com.google.common.base.Verify;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.service.PipeliteLockService;
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.resolver.ExceptionResolver;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.state.TaskExecutionState;
import pipelite.task.result.TaskExecutionResult;
import pipelite.stage.Stage;

@Slf4j
public class DefaultProcessLauncher implements ProcessLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService pipeliteLockService;
  private final PipeliteProcess pipeliteProcess;
  private final TaskExecutor executor;
  private final ExceptionResolver resolver;

  private boolean stop;

  public DefaultProcessLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfiguration processConfiguration,
      @Autowired TaskConfiguration taskConfiguration,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteStageService pipeliteStageService,
      @Autowired PipeliteLockService pipeliteLockService,
      PipeliteProcess pipeliteProcess) {

    Verify.verifyNotNull(launcherConfiguration);
    Verify.verifyNotNull(processConfiguration);
    Verify.verifyNotNull(taskConfiguration);
    Verify.verifyNotNull(pipeliteProcessService);
    Verify.verifyNotNull(pipeliteStageService);
    Verify.verifyNotNull(pipeliteLockService);
    Verify.verifyNotNull(pipeliteProcess);

    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.pipeliteStageService = pipeliteStageService;
    this.pipeliteLockService = pipeliteLockService;
    this.pipeliteProcess = pipeliteProcess;
    this.executor =
        processConfiguration
            .createExecutorFactory()
            .create(processConfiguration, taskConfiguration);
    this.resolver = processConfiguration.createResolver();
  }

  @Override
  public void run() {
    String threadName = Thread.currentThread().getName();
    ;
    try {
      Thread.currentThread()
          .setName(
              "ProcessLauncher: "
                  + getLauncherName()
                  + "/"
                  + getProcessName()
                  + "/"
                  + getProcessId());
      _run();
    } finally {
      Thread.currentThread().setName(threadName);
    }
  }

  private void _run() {
    if (stop) {
      return;
    }

    ProcessExecutionState state = pipeliteProcess.getState();

    if (state == null) {
      state = ProcessExecutionState.ACTIVE;
    }

    // Get a list of process tasks. If task A depends on task B then
    // task A will appear before task B.

    TaskInstance[] taskInstances = getTaskInstances();

    // Get process execution state from the tasks. If it is different from the
    // process execution state then update the project execution state to match.

    if (state != getProcessExecutionState(taskInstances)) {
      log.warn(
          "Process name {} process {} state {} changed to {}",
          getProcessName(),
          getProcessId(),
          state,
          getProcessExecutionState(taskInstances));

      pipeliteProcess.setState(getProcessExecutionState(taskInstances));
      pipeliteProcessService.saveProcess(pipeliteProcess);
    }

    if (state != ProcessExecutionState.ACTIVE) {
      log.warn(
          "Process name {} process {} state {} is not active",
          getProcessName(),
          getProcessId(),
          state);

      // The process needs to be active to be executed.
      return;
    }

    // Lock the process for execution.

    if (!lockProcess()) {
      log.warn("Process name {} process {} could not be locked", getProcessName(), getProcessId());
      return;
    }
    try {

      // Execute tasks and save their states.

      execute(taskInstances);

      // Update and save the process state.

      pipeliteProcess.incrementExecutionCount();
      pipeliteProcess.setState(getProcessExecutionState(taskInstances));
      pipeliteProcessService.saveProcess(pipeliteProcess);

    } finally {
      // Unlock the process.

      unlockProcess();
    }
  }

  private boolean lockProcess() {
    return pipeliteLockService.lockProcess(getLauncherName(), pipeliteProcess);
  }

  private void unlockProcess() {
    if (pipeliteLockService.isProcessLocked(pipeliteProcess)) {
      pipeliteLockService.unlockProcess(getLauncherName(), pipeliteProcess);
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
      String stageName = stage.toString();

      Optional<PipeliteStage> pipeliteStage =
          pipeliteStageService.getSavedStage(processName, processId, stageName);

      // Create and save the task it if does not already exist.

      if (!pipeliteStage.isPresent()) {
        pipeliteStage =
            Optional.of(
                PipeliteStage.newExecution(
                    pipeliteProcess.getProcessId(),
                    pipeliteProcess.getProcessName(),
                    stage.toString()));

        pipeliteStageService.saveStage(pipeliteStage.get());
      }

      taskInstances[i] =
          new TaskInstance(pipeliteProcess, pipeliteStage.get(), taskConfiguration, stage);
    }

    return taskInstances;
  }

  private void execute(TaskInstance[] taskInstances) {
    for (TaskInstance taskInstance : taskInstances) {
      if (stop) {
        return;
      }

      // Do not execute task if it is already completed.

      if (TaskExecutionState.COMPLETED == taskInstance.evaluateTaskExecutionState()) {
        continue;
      }

      // Do not execute failed tasks or any tasks that depend on it.

      if (TaskExecutionState.FAILED == taskInstance.evaluateTaskExecutionState()) {
        return;
      }

      log.info(
          "Starting task execution. Process name {} process {} stage {} result type {} result {} execution count {}",
          getProcessName(),
          getProcessId(),
          taskInstance.getPipeliteStage().getStageName(),
          taskInstance.getPipeliteStage().getResultType(),
          taskInstance.getPipeliteStage().getResult(),
          taskInstance.getPipeliteStage().getExecutionCount());

      // Update the task state before execution.

      taskInstance.getPipeliteStage().retryExecution();
      pipeliteStageService.saveStage(taskInstance.getPipeliteStage());

      // Execute the task.

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
      pipeliteStageService.saveStage(taskInstance.getPipeliteStage());

      // Reset dependent tasks if they are not active.

      resetDependentTasks(taskInstances, taskInstance, false);

      if (result.isError()) {

        // Do not continue execution if task execution fails.

        log.error(
            "Failed task execution. Process name {} process {} stage {} result type {} result {} execution count {}",
            getProcessName(),
            getProcessId(),
            taskInstance.getPipeliteStage().getStageName(),
            taskInstance.getPipeliteStage().getResultType(),
            taskInstance.getPipeliteStage().getResult(),
            taskInstance.getPipeliteStage().getExecutionCount());
        break;
      }
    }
  }

  private void resetDependentTasks(
      TaskInstance[] taskInstances, TaskInstance resetTaskInstance, boolean reset) {
    for (TaskInstance taskInstance : taskInstances) {
      if (taskInstance.equals(resetTaskInstance)) {
        continue;
      }

      Stage stageDependsOn = taskInstance.getStage().getDependsOn();
      if (stageDependsOn != null
          && stageDependsOn.getStageName().equals(resetTaskInstance.getStage().getStageName())) {
        resetDependentTasks(taskInstances, taskInstance, true);
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
    return pipeliteProcess.getProcessName();
  }

  public String getProcessId() {
    return pipeliteProcess.getProcessId();
  }

  public Stage[] getStages() {
    return processConfiguration.getStageArray();
  }

  public ProcessExecutionState getState() {
    return pipeliteProcess.getState();
  }

  public Integer getExecutionCount() {
    return pipeliteProcess.getExecutionCount();
  }

  public void stop() {
    this.stop = true;
  }
}
