/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.service;

import static org.apache.commons.text.WordUtils.capitalizeFully;

import com.google.common.flogger.FluentLogger;
import io.micrometer.core.annotation.Timed;
import java.util.List;
import java.util.Optional;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.StageEntity;
import pipelite.entity.StageEntityId;
import pipelite.entity.StageLogEntity;
import pipelite.entity.StageLogEntityId;
import pipelite.exception.PipeliteException;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.log.LogKey;
import pipelite.repository.StageLogRepository;
import pipelite.repository.StageRepository;
import pipelite.service.annotation.RetryableDataAccess;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorSerializer;

@Service
@RetryableDataAccess
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class StageService {

  private final StageRepository repository;
  private final StageLogRepository logRepository;
  private final InternalErrorService internalErrorService;

  public StageService(
      @Autowired StageRepository repository,
      @Autowired StageLogRepository logRepository,
      @Autowired InternalErrorService internalErrorService) {
    this.repository = repository;
    this.logRepository = logRepository;
    this.internalErrorService = internalErrorService;
  }

  /**
   * Returns the saved stages.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return the saved stages
   */
  @Timed("pipelite.service")
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
  @Timed("pipelite.service")
  public Optional<StageEntity> getSavedStage(
      String pipelineName, String processId, String stageName) {
    return repository.findById(new StageEntityId(processId, pipelineName, stageName));
  }

  public enum PrepareExecutionResult {
    NEW,
    CONTINUE
  };
  /**
   * Prepares stage entity before the stage execution starts.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stage the stage
   * @return prepare execution result
   */
  @Timed("pipelite.service")
  public PrepareExecutionResult prepareExecution(
      String pipelineName, String processId, Stage stage) {
    Optional<StageEntity> savedStageEntity =
        repository.findById(new StageEntityId(processId, pipelineName, stage.getStageName()));

    if (savedStageEntity.isPresent()) {
      // Use saved stage entity.
      stage.setStageEntity(savedStageEntity.get());
      if (stage.isActive()) {
        // Attempt to deserialize executor.
        StageExecutorSerializer.deserializeExecutor(stage, internalErrorService);
      }
      return prepareExecution(stage, PrepareExecutionResult.CONTINUE);
    }

    // Create new stage execution.
    createExecution(pipelineName, processId, stage);
    return prepareExecution(stage, PrepareExecutionResult.NEW);
  }

  private PrepareExecutionResult prepareExecution(
      Stage stage, PrepareExecutionResult prepareExecutionResult) {
    if (stage.getStageEntity() == null) {
      throw new PipeliteException(
          "Failed to prepare stage execution because stage entity is missing");
    }
    logContext(log.atInfo(), stage)
        .log(capitalizeFully(prepareExecutionResult.name()) + " stage execution");
    return prepareExecutionResult;
  }

  /**
   * Called when stage execution is created. Saves the stage.
   *
   * @param stage the stage
   */
  private void createExecution(String pipelineName, String processId, Stage stage) {
    stage.setStageEntity(
        StageEntity.createExecution(pipelineName, processId, stage.getStageName()));
    saveStage(stage);
  }

  /**
   * Called when stage execution is started. Saves the stage.
   *
   * @param stage the stage
   */
  @Timed("pipelite.service")
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
  @Timed("pipelite.service")
  public void endExecution(Stage stage, StageExecutorResult result) {
    stage.incrementImmediateExecutionCount();
    StageEntity stageEntity = stage.getStageEntity();
    stageEntity.endExecution(result, stage.getExecutor().getExecutorParams().getPermanentErrors());
    saveStage(stage);
    saveStageLog(StageLogEntity.endExecution(stageEntity, result));
  }

  /**
   * Called when the stage execution is reset. Saves the stage.
   *
   * @param stage the stage
   */
  @Timed("pipelite.service")
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
  @Timed("pipelite.service")
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
  @Timed("pipelite.service")
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
  @Timed("pipelite.service")
  public void saveStageLog(StageLogEntity stageLogEntity) {
    if (stageLogEntity.getStageLog() != null && !stageLogEntity.getStageLog().isEmpty()) {
      logRepository.save(stageLogEntity);
    }
  }

  /**
   * Deletes the stage.
   *
   * @param stageEntity the stage to delete
   */
  @Timed("pipelite.service")
  public void delete(StageEntity stageEntity) {
    repository.delete(stageEntity);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, Stage stage) {
    return log.with(LogKey.PIPELINE_NAME, stage.getStageEntity().getPipelineName())
        .with(LogKey.PROCESS_ID, stage.getStageEntity().getProcessId())
        .with(LogKey.STAGE_NAME, stage.getStageName());
  }

  private FluentLogger.Api logContext(
      FluentLogger.Api log, String pipelineName, String processId, String stageName) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stageName);
  }
}
