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
import pipelite.process.ProcessExecutionState;

@Entity
@Table(name = "PIPELITE_PROCESS")
@IdClass(PipeliteProcessId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipeliteProcess {

  @Id
  @Column(name = "PROCESS_ID")
  private String processId;

  // TODO: change column name to PROCESS_NAME
  @Id
  @Column(name = "PIPELINE_NAME")
  private String processName;

  @Enumerated(EnumType.STRING)
  @Column(name = "STATE", length = 15, nullable = false)
  private ProcessExecutionState state;

  @Column(name = "EXEC_CNT", nullable = false)
  private Integer executionCount = 0;

  @Column(name = "PRIORITY")
  private Integer priority = 0;

  public void incrementExecutionCount() {
    ++executionCount;
  }

  public static PipeliteProcess newExecution(String processId, String processName, int priority) {
    PipeliteProcess pipeliteProcess = new PipeliteProcess();
    pipeliteProcess.setProcessId(processId);
    pipeliteProcess.setProcessName(processName);
    pipeliteProcess.setPriority(priority);
    pipeliteProcess.setExecutionCount(0);
    pipeliteProcess.setState(ProcessExecutionState.NEW);
    return pipeliteProcess;
  }
}
