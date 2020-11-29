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

import static pipelite.stage.StageExecutionResultType.*;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.*;
import pipelite.entity.StageEntity;
import pipelite.launcher.dependency.DependencyResolver;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.service.MailService;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

@Flogger
public class ProcessLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessService processService;
  private final StageService stageService;
  private final MailService mailService;
  private final String pipelineName;
  private final Process process;

  private final Set<String> activeStages = ConcurrentHashMap.newKeySet();
  private final Duration stageLaunchFrequency;
  private final Duration stagePollFrequency;

  private final AtomicLong stageFailedCount = new AtomicLong(0);
  private final AtomicLong stageSuccessCount = new AtomicLong(0);

  private final ExecutorService executorService;

  private final LocalDateTime startTime = LocalDateTime.now();

  public ProcessLauncher(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      ProcessService processService,
      StageService stageService,
      MailService mailService,
      String pipelineName,
      Process process) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(stageConfiguration, "Missing stage configuration");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(mailService, "Missing mail service");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");
    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.mailService = mailService;
    this.pipelineName = pipelineName;
    this.process = process;
    if (launcherConfiguration.getStageLaunchFrequency() != null) {
      this.stageLaunchFrequency = launcherConfiguration.getStageLaunchFrequency();
    } else {
      this.stageLaunchFrequency = LauncherConfiguration.DEFAULT_STAGE_LAUNCH_FREQUENCY;
    }
    if (launcherConfiguration.getStagePollFrequency() != null) {
      this.stagePollFrequency = launcherConfiguration.getStagePollFrequency();
    } else {
      this.stagePollFrequency = LauncherConfiguration.DEFAULT_STAGE_POLL_FREQUENCY;
    }
    this.executorService = Executors.newCachedThreadPool();
  }

  // TODO: orphaned saved stages
  /**
   * Executes the process and returns the process state.
   *
   * @return The process state.
   */
  public ProcessState run() {
    logContext(log.atInfo()).log("Running process launcher");
    beforeExecution();
    processService.startExecution(process.getProcessEntity());
    while (true) {
      logContext(log.atFine()).log("Executing stages");
      List<Stage> executableStages = DependencyResolver.getExecutableStages(process.getStages());
      if (activeStages.isEmpty() && executableStages.isEmpty()) {
        break;
      }
      for (Stage stage : executableStages) {
        boolean isActive = activeStages.contains(stage.getStageName());
        if (isActive) {
          // Do not execute this stage because it is already active.
          continue;
        }
        if (stage.getDependsOn() != null) {
          for (Stage dependsOn :
              DependencyResolver.getDependsOnStages(process.getStages(), stage)) {
            if (activeStages.contains(dependsOn.getStageName())) {
              isActive = true;
              break;
            }
          }
        }
        if (isActive) {
          // Do not execute this stage because a stage it depends on is active.
          continue;
        }
        activeStages.add(stage.getStageName());
        executorService.execute(
            () -> {
              try {
                StageLauncher stageLauncher =
                    new StageLauncher(
                        launcherConfiguration,
                        stageConfiguration,
                        stageService,
                        mailService,
                        pipelineName,
                        process,
                        stage);
                StageExecutionResult result = stageLauncher.run();
                if (result.isSuccess()) {
                  stageSuccessCount.incrementAndGet();
                } else {
                  stageFailedCount.incrementAndGet();
                }
              } catch (Exception ex) {
                logContext(log.atSevere())
                    .withCause(ex)
                    .log("Unexpected exception when executing stage");
              } finally {
                activeStages.remove(stage.getStageName());
              }
            });
      }
      try {
        Thread.sleep(stageLaunchFrequency.toMillis());
      } catch (InterruptedException ex) {
        logContext(log.atSevere()).log("Process launcher was interrupted");
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
    }

    return endExecution();
  }

  private void beforeExecution() {
    for (Stage stage : process.getStages()) {
      // Add default executor parameters.
      stage.getExecutorParams().add(stageConfiguration);
      // Get stage entity.
      StageEntity stageEntity =
          stageService.beforeExecution(pipelineName, process.getProcessId(), stage).get();
      stage.setStageEntity(stageEntity);
    }
  }

  private ProcessState endExecution() {
    ProcessState processState = evaluateProcessState(process.getStages());
    logContext(log.atInfo()).log("Process execution finished: %s", processState.name());
    processService.endExecution(pipelineName, process, processState);
    return processState;
  }

  public static ProcessState evaluateProcessState(List<Stage> stages) {
    int errorCount = 0;
    for (Stage stage : stages) {
      StageEntity stageEntity = stage.getStageEntity();
      StageExecutionResultType resultType = stageEntity.getResultType();
      if (resultType == SUCCESS) {
        continue;
      }
      if (DependencyResolver.isExecutableStage(stages, stage)) {
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

  public String getPipelineName() {
    return pipelineName;
  }

  public String getProcessId() {
    return process.getProcessId();
  }

  public LocalDateTime getStartTime() {
    return startTime;
  }

  public long getStageFailedCount() {
    return stageFailedCount.get();
  }

  public long getStageSuccessCount() {
    return stageSuccessCount.get();
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, getProcessId());
  }
}
