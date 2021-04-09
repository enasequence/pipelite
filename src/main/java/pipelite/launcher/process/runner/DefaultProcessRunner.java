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

import static pipelite.stage.StageState.PENDING;
import static pipelite.stage.StageState.SUCCESS;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.StageEntity;
import pipelite.exception.PipeliteException;
import pipelite.launcher.StageLauncher;
import pipelite.launcher.dependency.DependencyResolver;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.service.*;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorSerializer;
import pipelite.time.Time;

/** Executes a process and returns the process state. */
@Flogger
public class DefaultProcessRunner implements ProcessRunner {

  private final String serviceName;
  private final ExecutorConfiguration executorConfiguration;
  private final InternalErrorService internalErrorService;
  private final ProcessService processService;
  private final StageService stageService;
  private final MailService mailService;
  private final HealthCheckService healthCheckService;
  private final String pipelineName;
  private final Duration processRunnerFrequency;
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final Map<Stage, StageLauncher> activeStages = new ConcurrentHashMap<>();
  private Process process;
  private String processId;
  private ZonedDateTime startTime;

  public DefaultProcessRunner(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      String pipelineName) {
    Assert.notNull(pipeliteConfiguration, "Missing configuration");
    Assert.notNull(pipeliteServices, "Missing services");
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.serviceName = pipeliteConfiguration.service().getName();
    this.executorConfiguration = pipeliteConfiguration.executor();
    this.internalErrorService = pipeliteServices.internalError();
    this.processService = pipeliteServices.process();
    this.stageService = pipeliteServices.stage();
    this.mailService = pipeliteServices.mail();
    this.healthCheckService = pipeliteServices.healthCheck();
    this.pipelineName = pipelineName;
    this.processRunnerFrequency = pipeliteConfiguration.advanced().getProcessRunnerFrequency();
  }

  /**
   * Executes the process.
   *
   * @param process the process
   * @return process runner result
   */
  @Override
  public ProcessRunnerResult runProcess(Process process) {
    ProcessRunnerResult result = new ProcessRunnerResult();
    runProcess(process, result);
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
    while (true) {
      if (!healthCheckService.isDataSourceHealthy()) {
        logContext(log.atSevere())
            .log("Waiting data source to be healthy before starting new stages");
      } else {
        logContext(log.atFine()).log("Executing stages");
        List<Stage> executableStages =
            DependencyResolver.getImmediatelyExecutableStages(
                process.getStages(), activeStages.keySet());

        if (activeStages.isEmpty() && executableStages.isEmpty()) {
          logContext(log.atInfo()).log("No more executable stages");
          break;
        }
        runStages(executableStages, result);
      }

      try {
        Time.wait(processRunnerFrequency);
      } catch (PipeliteException ex) {
        executorService.shutdownNow();
      }
    }
  }

  private void runStages(List<Stage> executableStages, ProcessRunnerResult result) {
    executableStages.forEach(stage -> runStage(stage, result));
  }

  private void runStage(Stage stage, ProcessRunnerResult result) {
    StageLauncher stageLauncher = new StageLauncher(stageService, pipelineName, process, stage);
    activeStages.put(stage, stageLauncher);
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
              result.incrementStageSuccess();
            } else {
              mailService.sendStageExecutionMessage(process, stage);
              result.incrementStageFailed();
            }
          } catch (Exception ex) {
            // Catching exceptions here to allow other stages to continue execution.
            StageExecutorResult exceptionResult = StageExecutorResult.internalError(ex);
            stageService.endExecution(stage, exceptionResult);
            mailService.sendStageExecutionMessage(process, stage);
            result.incrementStageFailed();
            internalErrorService.saveInternalError(
                serviceName, pipelineName, processId, stage.getStageName(), this.getClass(), ex);
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
    stage.getExecutor().getExecutorParams().validate();
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

  @Override
  public void terminate() {
    activeStages.values().forEach(stageLauncher -> stageLauncher.terminate());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, processId);
  }
}
