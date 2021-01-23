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

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import javax.persistence.*;
import lombok.*;
import pipelite.json.Json;
import pipelite.process.ProcessState;

@Entity
@Table(name = "PIPELITE2_PROCESS")
@IdClass(ProcessEntityId.class)
@Data
@NoArgsConstructor
public class ProcessEntity {

  public static final int MIN_PRIORITY = 0;
  public static final int DEFAULT_PRIORITY = 5;
  public static final int MAX_PRIORITY = 9;

  @Id
  @Column(name = "PROCESS_ID")
  private String processId;

  @Id
  @Column(name = "PIPELINE_NAME")
  private String pipelineName;

  @Enumerated(EnumType.STRING)
  @Column(name = "STATE", length = 15, nullable = false)
  private ProcessState processState;

  @Column(name = "EXEC_CNT", nullable = false)
  private Integer executionCount = 0;

  @Column(name = "EXEC_START")
  private ZonedDateTime startTime;

  @Column(name = "EXEC_END")
  private ZonedDateTime endTime;

  @Column(name = "PRIORITY", nullable = false)
  private Integer priority = DEFAULT_PRIORITY;

  /**
   * Creates a new process.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param priority te process priority
   * @return the new process
   */
  public static ProcessEntity createExecution(
      String pipelineName, String processId, Integer priority) {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(processId);
    processEntity.setPipelineName(pipelineName);
    processEntity.setPriority(getBoundedPriority(priority));
    processEntity.setExecutionCount(0);
    processEntity.setProcessState(ProcessState.PENDING);
    return processEntity;
  }

  /**
   * Called when the process execution starts. Sets the process state to active and sets the
   * execution start time.
   */
  public void startExecution() {
    this.processState = ProcessState.ACTIVE;
    this.startTime = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.endTime = null;
  }

  /**
   * Called when the process execution ends. Sets the process state and the execution end time.
   * Increases the process execution count.
   *
   * @param state the process state
   */
  public void endExecution(ProcessState state) {
    this.processState = state;
    this.endTime = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    ++executionCount;
  }

  public static int getBoundedPriority(Integer priority) {
    if (priority == null) {
      return DEFAULT_PRIORITY;
    }
    if (priority > MAX_PRIORITY) {
      return MAX_PRIORITY;
    }
    if (priority < MIN_PRIORITY) {
      return MIN_PRIORITY;
    }
    return priority;
  }

  public String serialize() {
    return Json.serialize(this);
  }

  @Override
  public String toString() {
    return serialize();
  }
}
