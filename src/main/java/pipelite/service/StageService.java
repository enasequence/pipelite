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

import java.util.List;
import java.util.Optional;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.StageEntity;
import pipelite.entity.StageEntityId;
import pipelite.entity.StageLogEntity;
import pipelite.entity.StageLogEntityId;
import pipelite.exception.PipeliteException;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.executor.describe.cache.AwsBatchDescribeJobsCache;
import pipelite.executor.describe.cache.KubernetesDescribeJobsCache;
import pipelite.executor.describe.cache.LsfDescribeJobsCache;
import pipelite.repository.StageLogRepository;
import pipelite.repository.StageRepository;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Retryable(
    listeners = {"dataSourceRetryListener"},
    maxAttemptsExpression = "#{@dataSourceRetryConfiguration.getAttempts()}",
    backoff =
        @Backoff(
            delayExpression = "#{@dataSourceRetryConfiguration.getDelay()}",
            maxDelayExpression = "#{@dataSourceRetryConfiguration.getMaxDelay()}",
            multiplierExpression = "#{@dataSourceRetryConfiguration.getMultiplier()}"),
    exceptionExpression = "#{@dataSourceRetryConfiguration.recoverableException(#root)}")
@Flogger
public class StageService {

  private final StageRepository repository;
  private final StageLogRepository logRepository;

  private final LsfDescribeJobsCache lsfDescribeJobsCache;
  private final AwsBatchDescribeJobsCache awsBatchDescribeJobsCache;
  private final KubernetesDescribeJobsCache kubernetesDescribeJobsCache;

  public StageService(
      @Autowired StageRepository repository,
      @Autowired StageLogRepository logRepository,
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired InternalErrorService internalErrorService) {

    this.repository = repository;
    this.logRepository = logRepository;

    this.lsfDescribeJobsCache =
        new LsfDescribeJobsCache(serviceConfiguration, internalErrorService);
    this.awsBatchDescribeJobsCache =
        new AwsBatchDescribeJobsCache(serviceConfiguration, internalErrorService);
    this.kubernetesDescribeJobsCache =
        new KubernetesDescribeJobsCache(serviceConfiguration, internalErrorService);
  }

  /**
   * Returns the saved stages.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return the saved stages
   */
  public List<StageEntity> getSavedStages(String pipelineName, String processId) {
    return repository.findByPipelineNameAndProcessId(pipelineName, processId);
  }

  /**
   * Returns the saved stage.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stageName the stage name
   * @return the saved stage
   */
  public Optional<StageEntity> getSavedStage(
      String pipelineName, String processId, String stageName) {
    return repository.findById(new StageEntityId(processId, pipelineName, stageName));
  }

  /**
   * Assigns a stage entity to the stage. Uses a saved stage entity if it exists or creates a new
   * one. Saves the stage.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stage the stage
   */
  public void createExecution(String pipelineName, String processId, Stage stage) {
    // Uses saved stage if it exists.
    Optional<StageEntity> savedStageEntity =
        repository.findById(new StageEntityId(processId, pipelineName, stage.getStageName()));
    if (savedStageEntity.isPresent()) {
      stage.setStageEntity(savedStageEntity.get());
    }
    StageEntity.createExecution(pipelineName, processId, stage);
    if (stage.getStageEntity() == null) {
      throw new PipeliteException("Failed to create new stage entity");
    }
    saveStage(stage);
  }

  /**
   * Called when the stage execution starts. Saves the stage.
   *
   * @param stage the stage
   */
  public void startExecution(Stage stage) {
    stage.getStageEntity().startExecution();
    saveStage(stage);
    deleteStageLog(logRepository, stage);
  }

  /**
   * Called when the stage execution ends. Saves the stage and stage log.
   *
   * @param stage the stage
   * @param result the stage execution result
   */
  public void endExecution(Stage stage, StageExecutorResult result) {
    stage.incrementImmediateExecutionCount();
    StageEntity stageEntity = stage.getStageEntity();
    stageEntity.endExecution(result);
    saveStage(stage);
    saveStageLog(StageLogEntity.endExecution(stageEntity, result));
  }

  /**
   * Called when the stage execution is reset. Saves the stage.
   *
   * @param stage the stage
   */
  public void resetExecution(Stage stage) {
    stage.getStageEntity().resetExecution();
    saveStage(stage);
    deleteStageLog(logRepository, stage);
  }

  public static void deleteStageLog(StageLogRepository logRepository, Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    StageLogEntityId stageLogEntityId =
        new StageLogEntityId(
            stageEntity.getProcessId(), stageEntity.getPipelineName(), stageEntity.getStageName());
    if (logRepository.existsById(stageLogEntityId)) {
      logRepository.deleteById(stageLogEntityId);
    }
  }

  /**
   * Returns the saved stage log.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stageName the stage name
   * @return the saved stage log
   */
  public Optional<StageLogEntity> getSavedStageLog(
      String pipelineName, String processId, String stageName) {
    return logRepository.findById(new StageLogEntityId(processId, pipelineName, stageName));
  }

  public static void prepareSaveStage(Stage stage) {
    StageExecutor stageExecutor = stage.getExecutor();
    StageEntity stageEntity = stage.getStageEntity();
    stageEntity.setExecutorName(stageExecutor.getClass().getName());
    if (stageExecutor instanceof JsonSerializableExecutor) {
      stageEntity.setExecutorData(((JsonSerializableExecutor) stageExecutor).serialize());
    }
    if (stageExecutor.getExecutorParams() != null) {
      stageEntity.setExecutorParams(stageExecutor.getExecutorParams().serialize());
    }
  }

  /**
   * Saves the stage.
   *
   * @param stage the stage to save
   * @return the saved stage entity
   */
  public StageEntity saveStage(Stage stage) {
    prepareSaveStage(stage);
    StageEntity stageEntity = stage.getStageEntity();
    log.atFiner().log("Saving stage: " + stageEntity.toString());
    return repository.save(stageEntity);
  }

  /**
   * Saves the stage log.
   *
   * @param stageLogEntity the stage logs to save
   * @return the saved stage log
   */
  public StageLogEntity saveStageLog(StageLogEntity stageLogEntity) {
    return logRepository.save(stageLogEntity);
  }

  /**
   * Deletes the stage.
   *
   * @param stageEntity the stage to delete
   */
  public void delete(StageEntity stageEntity) {
    repository.delete(stageEntity);
  }

  public LsfDescribeJobsCache getLsfDescribeJobsCache() {
    return lsfDescribeJobsCache;
  }

  public AwsBatchDescribeJobsCache getAwsBatchDescribeJobsCache() {
    return awsBatchDescribeJobsCache;
  }

  public KubernetesDescribeJobsCache getKubernetesDescribeJobsCache() {
    return kubernetesDescribeJobsCache;
  }
}
