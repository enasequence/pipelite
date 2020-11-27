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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.flogger.Flogger;
import pipelite.json.Json;
import pipelite.stage.StageExecutionResult;

import javax.persistence.*;

@Entity
@Table(name = "PIPELITE_STAGE")
@IdClass(StageEntityId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Flogger
public class StageOutEntity {

  @Id
  @Column(name = "PROCESS_ID")
  private String processId;

  @Id
  @Column(name = "PIPELINE_NAME")
  private String pipelineName;

  @Id
  @Column(name = "STAGE_NAME")
  private String stageName;

  @Column(name = "EXEC_STDOUT", columnDefinition = "CLOB")
  @Lob
  private String stdOut;

  @Column(name = "EXEC_STDERR", columnDefinition = "CLOB")
  @Lob
  private String stdErr;

  /** Stage execution starts. */
  public static StageOutEntity startExecution(StageEntity stageEntity) {
    StageOutEntity stageOutEntity = new StageOutEntity();
    stageOutEntity.pipelineName = stageEntity.getPipelineName();
    stageOutEntity.processId = stageEntity.getProcessId();
    stageOutEntity.stageName = stageEntity.getStageName();
    stageOutEntity.stdOut = null;
    stageOutEntity.stdErr = null;
    return stageOutEntity;
  }

  /** Stage execution ends. */
  public static StageOutEntity endExecution(StageEntity stageEntity, StageExecutionResult result) {
    StageOutEntity stageOutEntity = new StageOutEntity();
    stageOutEntity.pipelineName = stageEntity.getPipelineName();
    stageOutEntity.processId = stageEntity.getProcessId();
    stageOutEntity.stageName = stageEntity.getStageName();
    stageOutEntity.stdOut = result.getStdout();
    stageOutEntity.stdErr = result.getStderr();
    return stageOutEntity;
  }

  /** Reset stage execution. */
  public static StageOutEntity resetExecution(StageEntity stageEntity) {
    StageOutEntity stageOutEntity = new StageOutEntity();
    stageOutEntity.pipelineName = stageEntity.getPipelineName();
    stageOutEntity.processId = stageEntity.getProcessId();
    stageOutEntity.stageName = stageEntity.getStageName();
    stageOutEntity.stdOut = null;
    stageOutEntity.stdErr = null;
    return stageOutEntity;
  }

  public String serialize() {
    return Json.serialize(this);
  }

  @Override
  public String toString() {
    return serialize();
  }
}
