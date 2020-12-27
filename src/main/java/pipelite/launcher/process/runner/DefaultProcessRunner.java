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
import pipelite.exception.PipeliteInterruptedException;
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
  private final StageConfiguration stageConfiguration;
  private final ProcessService processService;
  private final StageService stageService;
  private final MailService mailService;
  private final Duration stageLaunchFrequency;
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private String pipelineName;
  private Process process;
  private String processId;
  private ZonedDateTime startTime;

  public DefaultProcessRunner(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      ProcessService processService,
      StageService stageService,
      MailService mailService) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(stageConfiguration, "Missing stage configuration");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(mailService, "Missing mail service");
    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.mailService = mailService;
    this.stageLaunchFrequency = launcherConfiguration.getStageLaunchFrequency();
  }

  // TODO: orphaned saved stages
  /** Executes the process and sets the new process state. */
  @Override
  public void runProcess(String pipelineName, Process process, ProcessRunnerCallback callback) {
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessId(), "Missing process id");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");
    Assert.notNull(callback, "Missing process runner callback");

    this.pipelineName = pipelineName;
    this.process = process;
    this.processId = process.getProcessId();
    this.startTime = ZonedDateTime.now();

    logContext(log.atInfo()).log("Executing process");

    ProcessRunnerResult result = new ProcessRunnerResult();
    startProcessExecution();
    executeProcess(result);
    endProcessExecution();
    result.setProcessExecutionCount(1);
    callback.accept(process, result);
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

      if (!Time.wait(stageLaunchFrequency)) {
        executorService.shutdownNow();
        throw new PipeliteInterruptedException("Process launcher was interrupted");
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
        new StageLauncher(stageConfiguration, pipelineName, process, stage);
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
              result.addStageSuccessCount(1);
            } else {
              mailService.sendStageExecutionMessage(process, stage);
              result.addStageFailedCount(1);
            }
          } catch (Exception ex) {
            stageService.endExecution(stage, StageExecutorResult.error(ex));
            mailService.sendStageExecutionMessage(process, stage);
            result.addStageExceptionCount(1);
            logContext(log.atSevere())
                .withCause(ex)
                .log("Unexpected exception when executing stage");
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
    // Use default executor parameters.
    stage.getExecutorParams().add(stageConfiguration);

    // Use saved stage or create a new one if it is missing.
    StageEntity stageEntity = stageService.getStage(pipelineName, process.getProcessId(), stage);
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
