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
package pipelite.launcher;

import static pipelite.stage.ConfigurableStageParameters.DEFAULT_RETRIES;
import static pipelite.stage.StageExecutionResultType.*;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.configuration.*;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.executor.PollableExecutor;
import pipelite.executor.SerializableExecutor;
import pipelite.executor.StageExecutor;
import pipelite.launcher.dependency.DependencyResolver;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessExecutionState;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

@Flogger
@Component()
@Scope("prototype")
public class ProcessLauncher implements Runnable {

  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessService processService;
  private final StageService stageService;
  private final List<StageAndStageEntity> stageAndStageEntities;
  private final DependencyResolver dependencyResolver;
  private final ExecutorService executorService;
  private final Set<String> activeStages = ConcurrentHashMap.newKeySet();
  private final Duration stageLaunchFrequency;

  public static final Duration DEFAULT_STAGE_LAUNCH_FREQUENCY = Duration.ofMinutes(1);

  private ProcessAndProcessEntity processAndProcessEntity;

  private final AtomicInteger stageFailedCount = new AtomicInteger(0);
  private final AtomicInteger stageCompletedCount = new AtomicInteger(0);

  public ProcessLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired StageConfiguration stageConfiguration,
      @Autowired ProcessService processService,
      @Autowired StageService stageService) {

    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.stageAndStageEntities = new ArrayList<>();
    this.dependencyResolver = new DependencyResolver(stageAndStageEntities);
    this.executorService = Executors.newCachedThreadPool();

    if (launcherConfiguration.getStageLaunchFrequency() != null) {
      this.stageLaunchFrequency = launcherConfiguration.getStageLaunchFrequency();
    } else {
      this.stageLaunchFrequency = DEFAULT_STAGE_LAUNCH_FREQUENCY;
    }
  }

  private static class ProcessAndProcessEntity {
    private final Process process;
    private final ProcessEntity processEntity;

    public ProcessAndProcessEntity(Process process, ProcessEntity processEntity) {
      this.process = process;
      this.processEntity = processEntity;
    }

    public Process getProcess() {
      return process;
    }

    public ProcessEntity getProcessEntity() {
      return processEntity;
    }
  }

  public static class StageAndStageEntity {
    private final Stage stage;
    private final StageEntity stageEntity;

    public StageAndStageEntity(Stage stage, StageEntity stageEntity) {
      this.stage = stage;
      this.stageEntity = stageEntity;
    }

    public Stage getStage() {
      return stage;
    }

    public StageEntity getStageEntity() {
      return stageEntity;
    }
  }

  public void init(Process process, ProcessEntity processEntity) {
    this.processAndProcessEntity = new ProcessAndProcessEntity(process, processEntity);
  }

  @Override
  public void run() {
    logContext(log.atInfo()).log("Running process launcher");
    createStages();
    executeStages();
    saveProcess();
  }

  // TODO: orphaned saved stages
  private void createStages() {
    Process process = processAndProcessEntity.getProcess();
    List<Stage> stages = process.getStages();

    for (Stage stage : stages) {
      stage.getStageParameters().add(stageConfiguration);

      Optional<StageEntity> processEntity =
          stageService.getSavedStage(
              process.getPipelineName(), process.getProcessId(), stage.getStageName());

      // Create the stage in database if it does not already exist.
      if (!processEntity.isPresent()) {
        processEntity = Optional.of(stageService.saveStage(StageEntity.createExecution(stage)));
      }

      stageAndStageEntities.add(new StageAndStageEntity(stage, processEntity.get()));
    }
  }

  private ProcessExecutionState evaluateProcessExecutionState() {
    int successCount = 0;
    for (StageAndStageEntity stageAndStageEntity : stageAndStageEntities) {

      StageExecutionResultType resultType = stageAndStageEntity.getStageEntity().getResultType();

      if (resultType == SUCCESS) {
        successCount++;
      } else if (resultType == null || resultType == ACTIVE) {
        return ProcessExecutionState.ACTIVE;
      } else {
        Integer executionCount = stageAndStageEntity.getStageEntity().getExecutionCount();

        int retries = DEFAULT_RETRIES;
        if (stageAndStageEntity.getStage().getStageParameters().getRetries() != null) {
          retries = stageAndStageEntity.getStage().getStageParameters().getRetries();
        }

        if (resultType == ERROR && executionCount != null && executionCount >= retries) {
          return ProcessExecutionState.FAILED;
        }
      }
    }

    if (successCount == stageAndStageEntities.size()) {
      return ProcessExecutionState.COMPLETED;
    }

    return ProcessExecutionState.ACTIVE;
  }

  private void executeStages() {
    while (true) {
      if (Thread.currentThread().isInterrupted()) {
        executorService.shutdownNow();
        return;
      }

      logContext(log.atFine()).log("Executing stages");

      List<StageAndStageEntity> runnableStages = dependencyResolver.getRunnableStages();
      if (runnableStages.isEmpty()) {
        return;
      }

      for (StageAndStageEntity stageAndStageEntity : runnableStages) {
        String stageName = stageAndStageEntity.getStage().getStageName();
        if (activeStages.contains(stageName)) {
          continue;
        }

        if (stageAndStageEntity.getStage().getDependsOn() != null) {
          String dependsOnStageName = stageAndStageEntity.getStage().getDependsOn().getStageName();
          if (dependsOnStageName != null && activeStages.contains(dependsOnStageName)) {
            continue;
          }
        }

        activeStages.add(stageName);
        executorService.execute(
            () -> {
              try {
                executeStage(stageAndStageEntity);
              } catch (Exception ex) {
                logContext(log.atSevere())
                    .withCause(ex)
                    .log("Unexpected exception when executing stage");
              } finally {
                activeStages.remove(stageName);
              }
            });
      }

      try {
        Thread.sleep(stageLaunchFrequency.toMillis());
      } catch (InterruptedException ex) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void saveProcess() {

    ProcessEntity processEntity = processAndProcessEntity.getProcessEntity();

    logContext(log.atInfo()).log("Saving process");

    processEntity.setState(evaluateProcessExecutionState());
    processEntity.incrementExecutionCount();

    processService.saveProcess(processEntity);
  }

  private void executeStage(StageAndStageEntity stageAndStageEntity) {
    Stage stage = stageAndStageEntity.getStage();
    StageEntity stageEntity = stageAndStageEntity.getStageEntity();
    String stageName = stage.getStageName();

    logContext(log.atInfo(), stageName).log("Executing stage");

    StageExecutionResult result = null;
    StageExecutor executor = null;

    // Resume stage execution.

    if (stageEntity.getResultType() == ACTIVE
        && stageEntity.getExecutorName() != null
        && stageEntity.getExecutorData() != null) {
      try {
        executor =
            SerializableExecutor.deserialize(
                stageEntity.getExecutorName(), stageEntity.getExecutorData());
        if (!(executor instanceof PollableExecutor)) {
          executor = null;
        }
      } catch (Exception ex) {
        logContext(log.atSevere(), stageName)
            .withCause(ex)
            .log("Failed to resume stage execution: %s", stageEntity.getExecutorName());
      }

      if (executor != null) {
        try {
          result = ((PollableExecutor) executor).poll(stage);
        } catch (Exception ex) {
          logContext(log.atSevere(), stageName)
              .withCause(ex)
              .log("Failed to resume stage execution: %s", stageEntity.getExecutorName());
        }
      }
    }

    if (result == null || result.isError()) {
      // Execute the stage.
      stageEntity.startExecution(stage);
      stageService.saveStage(stageEntity);

      try {
        result = stage.getExecutor().execute(stage);
      } catch (Exception ex) {
        result = StageExecutionResult.error();
        result.addExceptionAttribute(ex);
      }
    }

    if (result.isActive() && executor instanceof PollableExecutor) {
      // Save the stage executor details required for polling.
      stageService.saveStage(stageEntity);
      result = ((PollableExecutor) executor).poll(stage);
    }

    stageEntity.endExecution(result);
    stageService.saveStage(stageEntity);

    if (result.isSuccess()) {
      stageCompletedCount.incrementAndGet();
      logContext(log.atInfo(), stageEntity.getStageName())
          .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stageEntity.getResultType())
          .with(LogKey.STAGE_EXECUTION_COUNT, stageEntity.getExecutionCount())
          .log("Stage executed successfully.");
      invalidateDependentStages(stageAndStageEntity);
    } else {
      stageFailedCount.incrementAndGet();
      logContext(log.atSevere(), stageEntity.getStageName())
          .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stageEntity.getResultType())
          .with(LogKey.STAGE_EXECUTION_COUNT, stageEntity.getExecutionCount())
          .log("Stage execution failed");
    }
  }

  private void invalidateDependentStages(StageAndStageEntity from) {
    for (StageAndStageEntity stageAndStageEntity : dependencyResolver.getDependentStages(from)) {
      stageAndStageEntity.getStageEntity().resetExecution();
      stageService.saveStage(stageAndStageEntity.getStageEntity());
    }
  }

  public String getLauncherName() {
    return launcherConfiguration.getLauncherName();
  }

  public String getPipelineName() {
    return processAndProcessEntity.getProcess().getPipelineName();
  }

  public String getProcessId() {
    return processAndProcessEntity.getProcess().getProcessId();
  }

  public int getStageFailedCount() {
    return stageFailedCount.get();
  }

  public int getStageCompletedCount() {
    return stageCompletedCount.get();
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PIPELINE_NAME, getPipelineName())
        .with(LogKey.PROCESS_ID, getProcessId());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String stageName) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PIPELINE_NAME, getPipelineName())
        .with(LogKey.PROCESS_ID, getProcessId())
        .with(LogKey.STAGE_NAME, stageName);
  }
}
