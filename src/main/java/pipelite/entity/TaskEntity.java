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
import pipelite.executor.SerializableExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.Task;

@Entity
@Table(name = "PIPELITE_STAGE")
@IdClass(TaskEntityId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Flogger
public class TaskEntity {

  @Id
  @Column(name = "PROCESS_ID")
  private String processId;

  // TODO: change column name to PROCESS_NAME
  @Id
  @Column(name = "PIPELINE_NAME")
  private String processName;

  @Id
  @Column(name = "STAGE_NAME")
  private String taskName;

  @Column(name = "EXEC_CNT", nullable = false)
  private int executionCount = 0;

  @Column(name = "EXEC_START")
  private LocalDateTime startTime;

  @Column(name = "EXEC_DATE")
  private LocalDateTime endTime;

  @Column(name = "EXEC_STDOUT")
  @Lob
  private String stdOut;

  @Column(name = "EXEC_STDERR")
  @Lob
  private String stdErr;

  @Column(name = "EXEC_NAME")
  private String executorName;

  @Column(name = "EXEC_DATA")
  @Lob
  private String executorData;

  @Column(name = "EXEC_PARAMS")
  @Lob
  private String executorParams;

  @Enumerated(EnumType.STRING)
  @Column(name = "EXEC_RESULT_TYPE", length = 15)
  private TaskExecutionResultType resultType;

  @Column(name = "EXEC_RESULT_PARAMS")
  @Lob
  private String resultParams;

  public static TaskEntity createExecution(Task task) {
    String processId = task.getProcessId();
    String processName = task.getProcessName();
    String taskName = task.getTaskName();
    TaskEntity taskEntity = new TaskEntity();
    taskEntity.setProcessId(processId);
    taskEntity.setProcessName(processName);
    taskEntity.setTaskName(taskName);
    taskEntity.setResultType(TaskExecutionResultType.NEW);
    taskEntity.setExecutionCount(0);
    return taskEntity;
  }

  public void startExecution(Task task) {
    TaskExecutor taskExecutor = task.getExecutor();
    this.resultType = TaskExecutionResultType.ACTIVE;
    this.resultParams = null;
    this.startTime = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.endTime = null;
    this.stdOut = null;
    this.stdErr = null;
    if (taskExecutor instanceof SerializableExecutor) {
      this.executorName = taskExecutor.getClass().getName();
      this.executorData = ((SerializableExecutor) taskExecutor).serialize();
    }
    if (task.getTaskParameters() != null) {
      this.executorParams = task.getTaskParameters().json();
    }
  }

  public void endExecution(TaskExecutionResult result) {
    this.resultType = result.getResultType();
    this.resultParams = result.attributesJson();
    this.endTime = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.stdOut = result.getStdout();
    this.stdErr = result.getStderr();
    this.executionCount++;
  }

  public void resetExecution() {
    this.resultType = TaskExecutionResultType.NEW;
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
