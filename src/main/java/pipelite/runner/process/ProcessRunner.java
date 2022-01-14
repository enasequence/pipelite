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
package pipelite.runner.process;

import static pipelite.stage.StageState.PENDING;
import static pipelite.stage.StageState.SUCCESS;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.StageEntity;
import pipelite.error.InternalErrorHandler;
import pipelite.exception.PipeliteProcessLockedException;
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.runner.stage.DependencyResolver;
import pipelite.runner.stage.StageRunner;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorSerializer;

/** Executes a process and returns the process state. */
@Flogger
public class ProcessRunner {

  private final String serviceName;
  private final ExecutorConfiguration executorConfiguration;
  private final PipeliteServices pipeliteServices;
  private final PipeliteMetrics pipeliteMetrics;
  private final String pipelineName;
  private final Process process;
  private final String processId;
  private ZonedDateTime startTime;
  private final Set<ActiveStageRunner> active = ConcurrentHashMap.newKeySet();
  private final InternalErrorHandler internalErrorHandler;

  @Data
  public static class ActiveStageRunner {
    private final Stage stage;
    @EqualsAndHashCode.Exclude private final StageRunner StageRunner;
    @EqualsAndHashCode.Exclude private Future<?> future;
  }

  public ProcessRunner(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      String pipelineName,
      Process process,
      boolean lockProcess) {
    Assert.notNull(pipeliteConfiguration, "Missing configuration");
    Assert.notNull(pipeliteServices, "Missing services");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessId(), "Missing process id");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");
    this.serviceName = pipeliteConfiguration.service().getName();
    this.executorConfiguration = pipeliteConfiguration.executor();
    this.pipeliteServices = pipeliteServices;
    this.pipeliteMetrics = pipeliteMetrics;
    this.pipelineName = pipelineName;
    this.process = process;
    this.processId = process.getProcessId();
    this.internalErrorHandler =
        new InternalErrorHandler(
            pipeliteServices.internalError(), serviceName, pipelineName, processId, this);
    if (lockProcess) {
      lockProcess(pipelineName);
    }
  }

  protected void lockProcess(String pipelineName) {
    // Lock process.
    if (!pipeliteServices.locker().lockProcess(pipelineName, processId)) {
      throw new PipeliteProcessLockedException(pipelineName, processId);
    }
  }

  protected void unlockProcess() {
    pipeliteServices.locker().unlockProcess(pipelineName, processId);
  }

  /**
   * Called until the process has been executed and the result callback has been called.
   *
   * @param resultCallback process runner result callback
   */
  public void runOneIteration(ProcessRunnerResultCallback resultCallback) {
    internalErrorHandler.execute(
        () -> {
          boolean isFirstIteration = startTime == null;
          ZonedDateTime runOneIterationStartTime = ZonedDateTime.now();
          if (isFirstIteration) {
            startTime = ZonedDateTime.now();
            startProcessExecution();
          }
          executeProcess(resultCallback);

          // ProcessRunner runOneIteration is called from AbstractScheduledService schedule.
          // It is guaranteed not to be called concurrently. We use an executor service to call
          // StageRunner runOneIteration. The StageRunner runOneIteration will not execute stages
          // in the same thread and should complete fairly quickly. We capture the StageRunner
          // runOneIteration future to make sure not to call StageRunner runOneIteration again
          // until the future has completed.
          runOneIterationForActiveStageRunners();
          pipeliteMetrics
              .getProcessRunnerOneIterationTimer()
              .record(Duration.between(runOneIterationStartTime, ZonedDateTime.now()));
        });
  }

  private void runOneIterationForActiveStageRunners() {
    active.stream()
        .filter(a -> a.getFuture() == null || a.getFuture().isDone())
        .forEach(a -> runOneIterationForActiveStageRunner(a));
  }

  private void runOneIterationForActiveStageRunner(ActiveStageRunner activeStageRunner) {
    activeStageRunner.setFuture(
        pipeliteServices
            .executor()
            .runStage()
            .submit(
                () -> {
                  internalErrorHandler.execute(
                      () ->
                          activeStageRunner
                              .getStageRunner()
                              .runOneIteration(
                                  (result) ->
                                      stageRunnerEndExecutionHandler(activeStageRunner, result)));
                }));
  }

  private void stageRunnerEndExecutionHandler(
      ActiveStageRunner activeStageRunner, StageExecutorResult result) {
    internalErrorHandler.execute(() -> endStageExecution(activeStageRunner.getStage(), result));
    active.remove(activeStageRunner);
  }

  private void startProcessExecution() {
    logContext(log.atInfo()).log("Executing process");
    startStagesExecution();
    pipeliteServices.process().startExecution(process.getProcessEntity());
  }

  private void executeProcess(ProcessRunnerResultCallback resultCallback) {
    logContext(log.atFine()).log("Executing stages");
    List<Stage> activeStages = active.stream().map(a -> a.getStage()).collect(Collectors.toList());
    List<Stage> executableStages =
        DependencyResolver.getImmediatelyExecutableStages(process.getStages(), activeStages);

    if (activeStages.isEmpty() && executableStages.isEmpty()) {
      logContext(log.atInfo()).log("No more executable stages");
      endProcessExecution();
      unlockProcess();
      pipeliteMetrics
          .pipeline(pipelineName)
          .process()
          .endProcessExecution(process.getProcessEntity().getProcessState());
      resultCallback.accept(process);
    }
    runStages(executableStages);
  }

  private void runStages(List<Stage> executableStages) {
    executableStages.forEach(stage -> runStage(stage));
  }

  private void runStage(Stage stage) {
    if (!StageExecutorSerializer.deserializeExecution(stage)) {
      pipeliteServices.stage().startExecution(stage);
    }
    StageRunner stageRunner =
        new StageRunner(
            pipeliteServices, pipeliteMetrics, serviceName, pipelineName, process, stage);
    ActiveStageRunner activeStageRunner = new ActiveStageRunner(stage, stageRunner);
    active.add(activeStageRunner);
  }

  private void endProcessExecution() {
    ProcessState processState = evaluateProcessState(process.getStages());
    logContext(log.atInfo()).log("Process execution finished: %s", processState.name());
    pipeliteServices.process().endExecution(process, processState);
  }

  private void startStagesExecution() {
    for (Stage stage : process.getStages()) {
      startStageExecution(stage);
    }
  }

  private void startStageExecution(Stage stage) {
    // Apply default executor parameters.
    stage.getExecutor().getExecutorParams().applyDefaults(executorConfiguration);
    stage.getExecutor().getExecutorParams().validate();
    StageEntity stageEntity =
        pipeliteServices.stage().createExecution(pipelineName, process.getProcessId(), stage);
    stage.setStageEntity(stageEntity);
  }

  private void endStageExecution(Stage stage, StageExecutorResult result) {
    if (result.isSuccess()) {
      resetDependentStageExecution(process, stage);
    }
  }

  /**
   * Evaluates the process state using the stage execution result types.
   *
   * @param stages list of stages
   * @return the process state
   */
  public static ProcessState evaluateProcessState(List<Stage> stages) {
    int errorCount = 0;
    for (Stage stage : stages) {
      StageEntity stageEntity = stage.getStageEntity();
      StageState stageState = stageEntity.getStageState();
      if (stageState == SUCCESS) {
        continue;
      }
      if (DependencyResolver.isEventuallyExecutableStage(stages, stage)) {
        return ProcessState.ACTIVE;
      } else {
        errorCount++;
      }
    }
    if (errorCount > 0) {
      return ProcessState.FAILED;
    }
    return ProcessState.COMPLETED;
  }

  /**
   * Resets the stage execution of all dependent stages.
   *
   * @param from the stage that has dependent stages
   */
  private void resetDependentStageExecution(Process process, Stage from) {
    for (Stage stage : DependencyResolver.getDependentStages(process.getStages(), from)) {
      if (stage.getStageEntity().getStageState() != PENDING) {
        pipeliteServices.stage().resetExecution(stage);
      }
    }
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public String getProcessId() {
    return processId;
  }

  public Process getProcess() {
    return process;
  }

  public ZonedDateTime getStartTime() {
    return startTime;
  }

  /** Terminates the process execution. Process execution can't be continued later. */
  public void terminate() {
    active.forEach(a -> a.getStageRunner().terminate());
    unlockProcess();
  }

  /** Detaches from the process execution. Asynchronous process execution can be continued later. */
  public void detach() {
    active.forEach(a -> a.getFuture().cancel(true));
    unlockProcess();
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, processId);
  }
}
