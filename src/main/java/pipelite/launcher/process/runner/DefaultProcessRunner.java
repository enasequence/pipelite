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
package pipelite.launcher.process.runner;

import static pipelite.stage.executor.StageExecutorResultType.*;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.*;
import pipelite.entity.StageEntity;
import pipelite.exception.PipeliteException;
import pipelite.launcher.StageLauncher;
import pipelite.launcher.dependency.DependencyResolver;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.service.MailService;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultType;
import pipelite.stage.executor.StageExecutorSerializer;
import pipelite.time.Time;

/** Executes a process and returns the process state. */
@Flogger
public class DefaultProcessRunner implements ProcessRunner {

  private final LauncherConfiguration launcherConfiguration;
  private final ExecutorConfiguration executorConfiguration;
  private final ProcessService processService;
  private final StageService stageService;
  private final MailService mailService;
  private final String pipelineName;
  private final Duration processRunnerFrequency;
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private Process process;
  private String processId;
  private ZonedDateTime startTime;

  public DefaultProcessRunner(
      LauncherConfiguration launcherConfiguration,
      ExecutorConfiguration executorConfiguration,
      ProcessService processService,
      StageService stageService,
      MailService mailService,
      String pipelineName) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(executorConfiguration, "Missing stage configuration");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(mailService, "Missing mail service");
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.launcherConfiguration = launcherConfiguration;
    this.executorConfiguration = executorConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.mailService = mailService;
    this.pipelineName = pipelineName;
    this.processRunnerFrequency = launcherConfiguration.getProcessRunnerFrequency();
  }

  /**
   * Executes the process. If an exception is thrown then it is caught and logged after incrementing
   * the internal error count of the returned ProcessRunnerResult.
   *
   * @param process the process
   * @return process runner result
   */
  @Override
  public ProcessRunnerResult runProcess(Process process) {
    ProcessRunnerResult result = new ProcessRunnerResult();
    try {
      runProcess(process, result);
    } catch (Exception ex) {
      result.internalError();
      logContext(log.atSevere()).withCause(ex).log("Unexpected exception when executing process");
    }
    return result;
  }

  public void runProcess(Process process, ProcessRunnerResult result) {
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessId(), "Missing process id");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");

    this.process = process;
    this.processId = process.getProcessId();
    this.startTime = ZonedDateTime.now();

    logContext(log.atInfo()).log("Executing process");

    startProcessExecution();
    executeProcess(result);
    endProcessExecution();
  }

  private void executeProcess(ProcessRunnerResult result) {
    Set<Stage> activeStages = ConcurrentHashMap.newKeySet();
    while (true) {
      logContext(log.atFine()).log("Executing stages");
      List<Stage> executableStages =
          DependencyResolver.getImmediatelyExecutableStages(process.getStages(), activeStages);

      if (activeStages.isEmpty() && executableStages.isEmpty()) {
        logContext(log.atInfo()).log("No more executable stages");
        break;
      }

      runStages(activeStages, executableStages, result);

      try {
        Time.wait(processRunnerFrequency);
      } catch (PipeliteException ex) {
        executorService.shutdownNow();
      }
    }
  }

  private void runStages(
      Set<Stage> activeStages, List<Stage> executableStages, ProcessRunnerResult result) {
    executableStages.forEach(
        stage -> {
          runStage(stage, activeStages, result);
        });
  }

  private void runStage(Stage stage, Set<Stage> activeStages, ProcessRunnerResult result) {
    StageLauncher stageLauncher =
        new StageLauncher(executorConfiguration, pipelineName, process, stage);
    activeStages.add(stage);
    executorService.execute(
        () -> {
          try {
            if (!StageExecutorSerializer.deserializeExecution(stage)) {
              stageService.startExecution(stage);
            }
            StageExecutorResult stageExecutorResult = stageLauncher.run();
            stageService.endExecution(stage, stageExecutorResult);
            if (stageExecutorResult.isSuccess()) {
              resetDependentStageExecution(process, stage);
              result.stageSuccess();
            } else {
              mailService.sendStageExecutionMessage(process, stage);
              result.stageFailed();
            }
          } catch (Exception ex) {
            stageService.endExecution(stage, StageExecutorResult.error(ex));
            mailService.sendStageExecutionMessage(process, stage);
            result.internalError();
            logContext(log.atSevere())
                .withCause(ex)
                .log("Unexpected exception when executing stage %s", stage.getStageName());
          } finally {
            activeStages.remove(stage);
          }
        });
  }

  private void startProcessExecution() {
    startStagesExecution();
    processService.startExecution(process.getProcessEntity());
  }

  private void startStagesExecution() {
    for (Stage stage : process.getStages()) {
      startStageExecution(stage);
    }
  }

  private void startStageExecution(Stage stage) {
    // Apply default executor parameters.
    stage.getExecutor().getExecutorParams().applyDefaults(executorConfiguration);
    StageEntity stageEntity =
        stageService.createExecution(pipelineName, process.getProcessId(), stage);
    stage.setStageEntity(stageEntity);
  }

  private void endProcessExecution() {
    ProcessState processState = evaluateProcessState(process.getStages());
    logContext(log.atInfo()).log("Process execution finished: %s", processState.name());
    processService.endExecution(process, processState);
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
      StageExecutorResultType resultType = stageEntity.getResultType();
      if (resultType == SUCCESS) {
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
      if (stage.getStageEntity().getResultType() != null) {
        stageService.resetExecution(stage);
      }
    }
  }

  @Override
  public String getPipelineName() {
    return pipelineName;
  }

  @Override
  public String getProcessId() {
    return processId;
  }

  @Override
  public Process getProcess() {
    return process;
  }

  @Override
  public ZonedDateTime getStartTime() {
    return startTime;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, processId);
  }
}
