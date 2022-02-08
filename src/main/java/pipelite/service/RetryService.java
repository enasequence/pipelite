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
package pipelite.service;

import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import pipelite.RegisteredPipeline;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.exception.PipeliteProcessRetryException;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.runner.schedule.ScheduleRunner;
import pipelite.runner.stage.DependencyResolver;
import pipelite.stage.Stage;

@Service
@Lazy
public class RetryService {

  private final RegisteredPipelineService registeredPipelineService;
  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final StageService stageService;
  private final RunnerService runnerService;

  public RetryService(
      @Autowired RegisteredPipelineService registeredPipelineService,
      @Autowired ScheduleService scheduleService,
      @Autowired ProcessService processService,
      @Autowired StageService stageService,
      @Autowired RunnerService runnerService) {
    this.registeredPipelineService = registeredPipelineService;
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.stageService = stageService;
    this.runnerService = runnerService;
  }

  /**
   * Retries a failed process.
   *
   * @param pipelineName the pipeline name
   * @param processId the processId
   * @throws PipeliteProcessRetryException if the process can't be retried
   */
  public void retry(String pipelineName, String processId) {

    processService.isRetryProcess(pipelineName, processId);
    boolean isRetrySchedule = scheduleService.isRetrySchedule(pipelineName, processId);

    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(pipelineName);
    Process process = ProcessFactory.create(processId, registeredPipeline);

    // Retry stages
    for (Stage stage : process.getStages()) {
      stage.setStageEntity(getSavedStage(pipelineName, processId, stage.getStageName()));
      if (DependencyResolver.isPermanentlyFailedStage(stage)) {
        stage.getStageEntity().resetExecution();
        stageService.saveStage(stage);
      }
    }

    // Retry process
    process.setProcessEntity(getSavedProcess(pipelineName, processId));
    processService.startExecution(process.getProcessEntity());

    // Retry schedule
    if (isRetrySchedule) {
      ScheduleRunner scheduler = runnerService.getScheduleRunner();
      if (scheduler == null) {
        throw new PipeliteProcessRetryException(pipelineName, processId, "missing scheduler");
      }
      scheduler.retrySchedule(pipelineName, processId);
    }
  }

  private ProcessEntity getSavedProcess(String pipelineName, String processId) {
    Optional<ProcessEntity> processEntity = processService.getSavedProcess(pipelineName, processId);
    if (!processEntity.isPresent()) {
      throw new PipeliteProcessRetryException(pipelineName, processId, "unknown process");
    }
    return processEntity.get();
  }

  private StageEntity getSavedStage(String pipelineName, String processId, String stageName) {
    Optional<StageEntity> stageEntity =
        stageService.getSavedStage(pipelineName, processId, stageName);
    if (!stageEntity.isPresent()) {
      throw new PipeliteProcessRetryException(
          pipelineName, processId, "unknown stage " + stageName);
    }
    return stageEntity.get();
  }
}
