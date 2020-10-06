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
import pipelite.process.ProcessState;

@Entity
@Table(name = "PIPELITE_PROCESS")
@IdClass(ProcessEntityId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProcessEntity {

  @Id
  @Column(name = "PROCESS_ID")
  private String processId;

  @Id
  @Column(name = "PIPELINE_NAME")
  private String pipelineName;

  @Enumerated(EnumType.STRING)
  @Column(name = "STATE", length = 15, nullable = false)
  private ProcessState state;

  @Column(name = "EXEC_CNT", nullable = false)
  private Integer executionCount = 0;

  @Column(name = "PRIORITY")
  private Integer priority;

  public void incrementExecutionCount() {
    ++executionCount;
  }

  public static ProcessEntity newExecution(String processId, String pipelineName, int priority) {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(processId);
    processEntity.setPipelineName(pipelineName);
    processEntity.setPriority(priority);
    processEntity.setExecutionCount(0);
    processEntity.setState(ProcessState.NEW);
    return processEntity;
  }
}
