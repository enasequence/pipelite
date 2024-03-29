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
package pipelite.entity;

import com.fasterxml.jackson.annotation.JsonRawValue;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.flogger.Flogger;
import pipelite.entity.field.ErrorType;
import pipelite.entity.field.StageState;
import pipelite.json.Json;
import pipelite.stage.executor.StageExecutorResult;

@Entity
@Table(name = "PIPELITE2_STAGE")
@IdClass(StageEntityId.class)
@Data
@NoArgsConstructor
@Flogger
public class StageEntity {

  @Id
  @Column(name = "PROCESS_ID", length = 256)
  private String processId;

  @Id
  @Column(name = "PIPELINE_NAME", length = 256)
  private String pipelineName;

  @Id
  @Column(name = "STAGE_NAME", length = 256)
  private String stageName;

  @Enumerated(EnumType.STRING)
  @Column(name = "STATE", length = 15, nullable = false)
  private StageState stageState;

  @Enumerated(EnumType.STRING)
  @Column(name = "ERROR_TYPE", length = 64)
  private ErrorType errorType;

  @Column(name = "EXEC_CNT", nullable = false)
  private int executionCount = 0;

  @Column(name = "EXEC_START")
  private ZonedDateTime startTime;

  @Column(name = "EXEC_END")
  private ZonedDateTime endTime;

  @Column(name = "EXEC_NAME")
  private String executorName;

  @Column(name = "EXEC_DATA", columnDefinition = "CLOB")
  @JsonRawValue
  private String executorData;

  @Column(name = "EXEC_PARAMS", columnDefinition = "CLOB")
  @JsonRawValue
  private String executorParams;

  @Column(name = "EXEC_RESULT_PARAMS", columnDefinition = "CLOB")
  @JsonRawValue
  private String resultParams;

  @Column(name = "EXIT_CODE")
  private Integer exitCode;

  /**
   * Creates a new stage entity and assigns it to the stage.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return the new stage entity
   */
  public static StageEntity createExecution(
      String pipelineName, String processId, String stageName) {
    StageEntity stageEntity = new StageEntity();
    stageEntity.setStageState(StageState.PENDING);
    stageEntity.setProcessId(processId);
    stageEntity.setPipelineName(pipelineName);
    stageEntity.setStageName(stageName);
    stageEntity.setExecutionCount(0);
    return stageEntity;
  }

  /** Called when the stage execution starts. */
  public void startExecution() {
    this.stageState = StageState.ACTIVE;
    this.errorType = null;
    this.startTime = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.endTime = null;
    this.resultParams = null;
    this.exitCode = null;
  }

  /**
   * Called when the stage execution ends.
   *
   * @param result the stage execution result
   * @param permanentErrors the permanent error exit codes
   */
  public void endExecution(StageExecutorResult result, List<Integer> permanentErrors) {
    this.stageState = StageState.from(result);
    this.errorType = ErrorType.from(result, permanentErrors);
    this.endTime = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.resultParams = result.attributesJson();
    String exitCodeAttribute = result.exitCode();
    try {
      this.exitCode = Integer.parseInt(exitCodeAttribute);
    } catch (Exception e) {
    }
    this.executionCount++;
  }

  /** Called when the stage execution is reset. */
  public void resetExecution() {
    this.stageState = StageState.PENDING;
    this.errorType = null;
    this.resultParams = null;
    this.startTime = null;
    this.endTime = null;
    this.executionCount = 0;
    this.executorName = null;
    this.executorData = null;
    this.executorParams = null;
  }

  public String serialize() {
    return Json.serialize(this);
  }

  @Override
  public String toString() {
    return serialize();
  }
}
