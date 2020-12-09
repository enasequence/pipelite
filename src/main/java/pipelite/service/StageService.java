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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.StageEntity;
import pipelite.entity.StageEntityId;
import pipelite.entity.StageOutEntity;
import pipelite.repository.StageOutRepository;
import pipelite.repository.StageRepository;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class StageService {

  private final StageRepository repository;
  private final StageOutRepository outRepository;

  public StageService(
      @Autowired StageRepository repository, @Autowired StageOutRepository outRepository) {
    this.repository = repository;
    this.outRepository = outRepository;
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
   * Returns the saved stage or creates a new one if it is missing. Called before stage execution
   * starts. Asynchronous executors may continue executing an interrupted stage.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stage the associated Stage class
   * @return the saved or created stage
   */
  public StageEntity getStage(String pipelineName, String processId, Stage stage) {
    // Get saved stage if it exists.
    Optional<StageEntity> stageEntity =
        repository.findById(new StageEntityId(processId, pipelineName, stage.getStageName()));
    if (!stageEntity.isPresent()) {
      // Save stage if it did not already exists.
      stageEntity =
          Optional.of(saveStage(StageEntity.createExecution(pipelineName, processId, stage)));
    }
    return stageEntity.get();
  }

  /**
   * Called when the stage execution starts to save the stage. This may allow asynchronous executors
   * to continue executing an interrupted stage.
   *
   * @param stage the associated Stage class
   */
  public void startExecution(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    stageEntity.startExecution(stage);
    saveStage(stageEntity);
    saveStageOut(StageOutEntity.startExecution(stageEntity));
  }

  /**
   * Called when the stage execution ends.
   *
   * @param stage the associated Stage class
   * @param result the stage execution result
   */
  public void endExecution(Stage stage, StageExecutionResult result) {
    stage.incrementImmediateExecutionCount();
    StageEntity stageEntity = stage.getStageEntity();
    stageEntity.endExecution(result);
    saveStage(stageEntity);
    saveStageOut(StageOutEntity.endExecution(stageEntity, result));
  }

  /**
   * Called when the stage execution is reset.
   *
   * @param stage the associated Stage class
   */
  public void resetExecution(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    stageEntity.resetExecution();
    saveStage(stageEntity);
    saveStageOut(StageOutEntity.resetExecution(stageEntity));
  }

  /**
   * Returns the saved stage output.
   *
   * @param stageEntity the stage
   * @return the saved stage output
   */
  public Optional<StageOutEntity> getSavedStageOut(StageEntity stageEntity) {
    return getSavedStageOut(
        stageEntity.getPipelineName(), stageEntity.getProcessId(), stageEntity.getStageName());
  }

  /**
   * Returns the saved stage output.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stageName the stage name
   * @return the saved stage output
   */
  public Optional<StageOutEntity> getSavedStageOut(
      String pipelineName, String processId, String stageName) {
    return outRepository.findById(new StageEntityId(processId, pipelineName, stageName));
  }

  /**
   * Saves the stage.
   *
   * @param stageEntity the stage to save
   * @return the saved stage
   */
  public StageEntity saveStage(StageEntity stageEntity) {
    return repository.save(stageEntity);
  }

  /**
   * Saves the stage output.
   *
   * @param stageOutEntity the stage output to save
   * @return the saved stage output
   */
  public StageOutEntity saveStageOut(StageOutEntity stageOutEntity) {
    return outRepository.save(stageOutEntity);
  }

  /**
   * Deletes the stage.
   *
   * @param stageEntity the stage to delete
   */
  public void delete(StageEntity stageEntity) {
    repository.delete(stageEntity);
  }
}
