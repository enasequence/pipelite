package pipelite.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import pipelite.process.ProcessExecutionState;

import javax.persistence.*;

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
  @Column(name = "STATE", length = 15)
  private ProcessExecutionState state;

  @Column(name = "EXEC_CNT")
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
