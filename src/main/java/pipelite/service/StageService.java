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
   * Returns the saved stage entity.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stageName the stage name
   * @return the saved stage entity
   */
  public Optional<StageEntity> getSavedStage(
      String pipelineName, String processId, String stageName) {
    return repository.findById(new StageEntityId(processId, pipelineName, stageName));
  }

  /**
   * Called to get the latest execution information before the stage execution starts. Asynchronous
   * executors may continue executing an active stage. Creates the stage if it is missing.
   */
  public Optional<StageEntity> beforeExecution(String pipelineName, String processId, Stage stage) {
    // Get saved stage if it exists.
    Optional<StageEntity> stageEntity =
        repository.findById(new StageEntityId(processId, pipelineName, stage.getStageName()));
    if (!stageEntity.isPresent()) {
      // Save stage if it did not already exists.
      stageEntity =
          Optional.of(saveStage(StageEntity.createExecution(pipelineName, processId, stage)));
    }
    return stageEntity;
  }

  /**
   * Called when the stage execution starts. Asynchronous executors may contain information that
   * allow them to continue executing an interrupted stage execution. This information is saved
   * here.
   */
  public void startExecution(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    stageEntity.startExecution(stage);
    saveStage(stageEntity);
    saveStageOut(StageOutEntity.startExecution(stageEntity));
  }

  public void endExecution(Stage stage, StageExecutionResult result) {
    stage.incrementImmediateExecutionCount();
    StageEntity stageEntity = stage.getStageEntity();
    stageEntity.endExecution(result);
    saveStage(stageEntity);
    saveStageOut(StageOutEntity.endExecution(stageEntity, result));
  }

  public void resetExecution(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    stageEntity.resetExecution();
    saveStage(stageEntity);
    saveStageOut(StageOutEntity.resetExecution(stageEntity));
  }

  public Optional<StageOutEntity> getSavedStageOut(StageEntity stageEntity) {
    return outRepository.findById(
        new StageEntityId(
            stageEntity.getProcessId(), stageEntity.getPipelineName(), stageEntity.getStageName()));
  }

  public StageEntity saveStage(StageEntity stageEntity) {
    return repository.save(stageEntity);
  }

  public StageOutEntity saveStageOut(StageOutEntity stageOutEntity) {
    return outRepository.save(stageOutEntity);
  }

  public void delete(StageEntity stageEntity) {
    repository.delete(stageEntity);
  }
}
