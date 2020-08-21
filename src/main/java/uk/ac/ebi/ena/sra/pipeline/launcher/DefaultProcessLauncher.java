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

import java.util.ArrayList;
import java.util.List;
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

  private String __name;

  private volatile boolean do_stop;

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

  public PipeliteProcess getPipeliteProcess() {
    return pipeliteProcess;
  }

  @Override
  public void run() {
    decorateThreadName();
    lifecycle();
    undecorateThreadName();
  }

  public void decorateThreadName() {
    __name = Thread.currentThread().getName();
    Thread.currentThread()
        .setName(Thread.currentThread().getName() + "@" + getProcessName() + "/" + getProcessId());
  }

  public void undecorateThreadName() {
    Thread.currentThread().setName(__name);
  }

  void lifecycle() {
    if (!do_stop) {
      TaskInstance[] taskInstances = createTaskInstances();

      ProcessExecutionState state = pipeliteProcess.getState();

      if (state == null) {
        state = ProcessExecutionState.ACTIVE;
      }

      if (state != getProcessExecutionState(taskInstances)) {
        log.warn(
            "Process name {} process {} state {} changed to {}",
            getProcessName(),
            getProcessId(),
            state,
            getProcessExecutionState(taskInstances));
        saveProcess(getProcessExecutionState(taskInstances));
      }

      if (state != ProcessExecutionState.ACTIVE) {
        log.warn(
            "Process name {} process {} state {} is not active",
            getProcessName(),
            getProcessId(),
            state);
        return;
      }

      if (!lockProcessInstance()) {
        log.warn(
            "Process name {} process {} could not be locked", getProcessName(), getProcessId());
        return;
      }
      try {
        execute(taskInstances);

        incrementProcessExecutionCount();

        saveProcess(getProcessExecutionState(taskInstances));

      } finally {
        unlockProcessInstance();
      }
    }
  }

  private void incrementProcessExecutionCount() {
    pipeliteProcess.incrementExecutionCount();
  }

  private boolean lockProcessInstance() {
    return pipeliteLockService.lockProcess(getLauncherName(), pipeliteProcess);
  }

  private void unlockProcessInstance() {
    if (pipeliteLockService.isProcessLocked(pipeliteProcess)) {
      pipeliteLockService.unlockProcess(getLauncherName(), pipeliteProcess);
    }
  }

  private ProcessExecutionState getProcessExecutionState(TaskInstance[] taskInstances) {
    for (TaskInstance taskInstance : taskInstances) {
      TaskExecutionState taskExecutionState = executor.getTaskExecutionState(taskInstance);

      log.info(
          "Process name {} process {} stage {} result {} state {} execution count {}",
          getProcessName(),
          getProcessId(),
          taskInstance.getPipeliteStage().getStageName(),
          taskInstance.getPipeliteStage().getResultType(),
          taskExecutionState,
          taskInstance.getPipeliteStage().getExecutionCount());
      switch (executor.getTaskExecutionState(taskInstance)) {
        case ACTIVE:
          return ProcessExecutionState.ACTIVE;
        case FAILED:
          return ProcessExecutionState.FAILED;
      }
    }
    return ProcessExecutionState.COMPLETED;
  }

  private void saveProcess(ProcessExecutionState processExecutionState) {
    pipeliteProcess.setState(processExecutionState);
    pipeliteProcessService.saveProcess(pipeliteProcess);
  }

  private TaskInstance[] createTaskInstances() {
    Stage[] stages = getStages();
    TaskInstance[] taskInstances = new TaskInstance[stages.length];

    for (int i = 0; i < taskInstances.length; ++i) {
      Stage stage = stages[i];
      String processId = pipeliteProcess.getProcessId();
      String processName = pipeliteProcess.getProcessName();
      String stageName = stage.toString();
      // Load stage it if already exists.
      Optional<PipeliteStage> pipeliteStage =
          pipeliteStageService.getSavedStage(processName, processId, stageName);
      if (!pipeliteStage.isPresent()) {
        // Create stage it if does not already exist.
        pipeliteStage =
            Optional.of(
                PipeliteStage.newExecution(
                    pipeliteProcess.getProcessId(),
                    pipeliteProcess.getProcessName(),
                    stage.toString()));
        // Save created stage.
        pipeliteStageService.saveStage(pipeliteStage.get());
      }
      TaskInstance instance =
          new TaskInstance(pipeliteProcess, pipeliteStage.get(), taskConfiguration, stage);
      taskInstances[i] = instance;
    }
    return taskInstances;
  }

  private void execute(TaskInstance[] taskInstances) {
    for (TaskInstance taskInstance : taskInstances) {
      if (do_stop) {
        break;
      }

      if (TaskExecutionState.COMPLETED == executor.getTaskExecutionState(taskInstance)) {
        continue;
      }

      if (TaskExecutionState.FAILED == executor.getTaskExecutionState(taskInstance)) {
        // TODO: re-start from failed state
        return;
      }

      taskInstance.getPipeliteStage().retryExecution();
      pipeliteStageService.saveStage(taskInstance.getPipeliteStage());

      executor.execute(taskInstance);

      ExecutionInfo info = executor.get_info();

      // Translate execution result to exec status
      TaskExecutionResult result;
      if (null != info.getThrowable()) {
        result = resolver.resolveError(info.getThrowable());
      } else {
        result = resolver.exitCodeSerializer().deserialize(info.getExitCode());
      }

      taskInstance
          .getPipeliteStage()
          .endExecution(result, info.getCommandline(), info.getStdout(), info.getStderr());

      pipeliteStageService.saveStage(taskInstance.getPipeliteStage());

      List<TaskInstance> dependend = invalidate_dependands(taskInstances, taskInstance);
      for (TaskInstance si : dependend) {
        pipeliteStageService.saveStage(si.getPipeliteStage());
      }

      if (result.isError()) {
        log.error(
            "Error executing Unable to load process {} stage {} for process {}",
            pipeliteProcess.getProcessName(),
            taskInstance.getStage().getStageName(),
            pipeliteProcess.getProcessId());
        break;
      }
    }
  }

  private List<TaskInstance> invalidate_dependands(
      TaskInstance[] taskInstances, TaskInstance fromTaskInstance) {
    List<TaskInstance> result = new ArrayList<>(getStages().length);
    invalidate_dependands(taskInstances, fromTaskInstance, false, result);
    return result;
  }

  private void invalidate_dependands(
      TaskInstance[] taskInstances,
      TaskInstance fromTaskInstance,
      boolean reset,
      List<TaskInstance> touched) {
    for (TaskInstance taskInstance : taskInstances) {
      if (taskInstance.equals(fromTaskInstance)) {
        continue;
      }

      Stage stageDependsOn = taskInstance.getStage().getDependsOn();
      if (stageDependsOn != null
          && stageDependsOn.getStageName().equals(fromTaskInstance.getStage().getStageName())) {
        invalidate_dependands(taskInstances, taskInstance, true, touched);
      }
    }

    if (reset) {
      executor.reset(fromTaskInstance);
      touched.add(fromTaskInstance);
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

  public void stop() {
    this.do_stop = true;
  }
}
