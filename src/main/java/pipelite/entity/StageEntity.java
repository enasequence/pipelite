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

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import javax.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.flogger.Flogger;
import pipelite.executor.StageExecutor;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

@Entity
@Table(name = "PIPELITE_STAGE")
@IdClass(StageEntityId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Flogger
public class StageEntity {

  @Id
  @Column(name = "PROCESS_ID")
  private String processId;

  @Id
  @Column(name = "PIPELINE_NAME")
  private String pipelineName;

  @Id
  @Column(name = "STAGE_NAME")
  private String stageName;

  @Column(name = "EXEC_CNT", nullable = false)
  private int executionCount = 0;

  @Column(name = "EXEC_START")
  private LocalDateTime startTime;

  @Column(name = "EXEC_DATE")
  private LocalDateTime endTime;

  @Column(name = "EXEC_STDOUT", columnDefinition = "CLOB")
  @Lob
  private String stdOut;

  @Column(name = "EXEC_STDERR", columnDefinition = "CLOB")
  @Lob
  private String stdErr;

  @Column(name = "EXEC_NAME")
  private String executorName;

  @Column(name = "EXEC_DATA", columnDefinition = "CLOB")
  @Lob
  private String executorData;

  @Column(name = "EXEC_PARAMS", columnDefinition = "CLOB")
  @Lob
  private String executorParams;

  @Enumerated(EnumType.STRING)
  @Column(name = "EXEC_RESULT_TYPE", length = 15)
  private StageExecutionResultType resultType;

  @Column(name = "EXEC_RESULT_PARAMS", columnDefinition = "CLOB")
  @Lob
  private String resultParams;

  /** Prepare stage for execution. */
  public static StageEntity createExecution(Stage stage) {
    String processId = stage.getProcessId();
    String pipelineName = stage.getPipelineName();
    String stageName = stage.getStageName();
    StageEntity stageEntity = new StageEntity();
    stageEntity.setProcessId(processId);
    stageEntity.setPipelineName(pipelineName);
    stageEntity.setStageName(stageName);
    stageEntity.setResultType(StageExecutionResultType.NEW);
    stageEntity.setExecutionCount(0);
    return stageEntity;
  }

  /** Stage execution starts. */
  public void startExecution(Stage stage) {
    StageExecutor stageExecutor = stage.getExecutor();
    this.resultType = StageExecutionResultType.ACTIVE;
    this.resultParams = null;
    this.startTime = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.endTime = null;
    this.stdOut = null;
    this.stdErr = null;
    this.executorName = stageExecutor.getClass().getName();
    this.executorData = stageExecutor.serialize();
    if (stage.getStageParameters() != null) {
      this.executorParams = stage.getStageParameters().json();
    }
  }

  /** Asynchronous stage execution starts. Save information required to resume execution. */
  public void asyncExecution(Stage stage) {
    StageExecutor stageExecutor = stage.getExecutor();
    this.executorData = stageExecutor.serialize();
  }

  /** Stage execution ends. */
  public void endExecution(StageExecutionResult result) {
    this.resultType = result.getResultType();
    this.resultParams = result.attributesJson();
    this.endTime = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.stdOut = result.getStdout();
    this.stdErr = result.getStderr();
    this.executionCount++;
  }

  /** Reset stage execution. */
  public void resetExecution() {
    this.resultType = StageExecutionResultType.NEW;
    this.resultParams = null;
    this.startTime = null;
    this.endTime = null;
    this.stdOut = null;
    this.stdErr = null;
    this.executionCount = 0;
    this.executorName = null;
    this.executorData = null;
    this.executorParams = null;
  }
}
