package pipelite.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import pipelite.process.state.ProcessExecutionState;

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
  private ProcessExecutionState state = ProcessExecutionState.ACTIVE;

  @Column(name = "EXEC_CNT")
  private Integer executionCount = 0;

  @Column(name = "PRIORITY")
  private Integer priority;

  public void incrementExecutionCount() {
    ++executionCount;
  }
}
