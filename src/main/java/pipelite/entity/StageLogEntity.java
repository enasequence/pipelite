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
package pipelite.entity;

import javax.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.flogger.Flogger;
import pipelite.json.Json;
import pipelite.stage.executor.StageExecutorResult;

@Entity
@Table(name = "PIPELITE_STAGE_LOG")
@IdClass(StageLogEntityId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Flogger
public class StageLogEntity {

  @Id
  @Column(name = "PROCESS_ID")
  private String processId;

  @Id
  @Column(name = "PIPELINE_NAME")
  private String pipelineName;

  @Id
  @Column(name = "STAGE_NAME")
  private String stageName;

  @Column(name = "STAGE_LOG", columnDefinition = "CLOB")
  @Lob
  private String stageLog;

  /** Stage execution starts. */
  public static StageLogEntity startExecution(StageEntity stageEntity) {
    StageLogEntity stageLogEntity = new StageLogEntity();
    stageLogEntity.pipelineName = stageEntity.getPipelineName();
    stageLogEntity.processId = stageEntity.getProcessId();
    stageLogEntity.stageName = stageEntity.getStageName();
    stageLogEntity.stageLog = null;
    return stageLogEntity;
  }

  /** Stage execution ends. */
  public static StageLogEntity endExecution(StageEntity stageEntity, StageExecutorResult result) {
    StageLogEntity stageLogEntity = new StageLogEntity();
    stageLogEntity.pipelineName = stageEntity.getPipelineName();
    stageLogEntity.processId = stageEntity.getProcessId();
    stageLogEntity.stageName = stageEntity.getStageName();
    stageLogEntity.stageLog = result.getStageLog();
    return stageLogEntity;
  }

  /** Reset stage execution. */
  public static StageLogEntity resetExecution(StageEntity stageEntity) {
    StageLogEntity stageLogEntity = new StageLogEntity();
    stageLogEntity.pipelineName = stageEntity.getPipelineName();
    stageLogEntity.processId = stageEntity.getProcessId();
    stageLogEntity.stageName = stageEntity.getStageName();
    stageLogEntity.stageLog = null;
    return stageLogEntity;
  }

  public String serialize() {
    return Json.serialize(this);
  }

  @Override
  public String toString() {
    return serialize();
  }
}
