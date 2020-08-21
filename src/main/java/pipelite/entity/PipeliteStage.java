package pipelite.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import pipelite.task.result.TaskExecutionResult;
import pipelite.task.result.TaskExecutionResultType;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "PIPELITE_STAGE")
@IdClass(PipeliteStageId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipeliteStage {

  @Id
  @Column(name = "PROCESS_ID")
  private String processId;

  // TODO: change column name to PROCESS_NAME
  @Id
  @Column(name = "PIPELINE_NAME")
  private String processName;

  @Id
  @Column(name = "STAGE_NAME")
  private String stageName;

  @Enumerated(EnumType.STRING)
  @Column(name = "EXEC_RESULT_TYPE", length = 15)
  private TaskExecutionResultType resultType;

  @Column(name = "EXEC_RESULT")
  private String result;

  @Column(name = "EXEC_CNT")
  private Integer executionCount;

  @Column(name = "EXEC_START")
  private LocalDateTime startTime;

  // TODO: change column name to EXEC_END
  @Column(name = "EXEC_DATE")
  private LocalDateTime endTime;

  // TODO: lazy or better to move to another table
  @Column(name = "EXEC_CMD_LINE")
  @Lob
  private String executionCmd;

  // TODO: lazy or better to move to another table
  @Column(name = "EXEC_STDOUT")
  @Lob
  private String stdOut;

  // TODO: lazy or better to move to another table
  @Column(name = "EXEC_STDERR")
  @Lob
  private String stdErr;

  @Column(name = "ENABLED")
  @Convert(converter = BooleanConverter.class)
  private Boolean enabled = true;

  public static PipeliteStage newExecution(String processId, String processName, String stageName) {
    PipeliteStage pipeliteStage = new PipeliteStage();
    pipeliteStage.setProcessId(processId);
    pipeliteStage.setProcessName(processName);
    pipeliteStage.setStageName(stageName);
    pipeliteStage.setExecutionCount(0);
    return pipeliteStage;
  }

  public void resetExecution() {
    this.resultType = null;
    this.result = null;
    this.startTime = null;
    this.endTime = null;
    this.executionCmd = null;
    this.stdOut = null;
    this.stdErr = null;
    this.executionCount = 0;
  }

  public void retryExecution() {
    this.resultType = null;
    this.result = null;
    this.startTime = LocalDateTime.now();
    this.endTime = null;
    this.executionCmd = null;
    this.stdOut = null;
    this.stdErr = null;
  }

  public void endExecution(
      TaskExecutionResult result, String executionCmd, String stdOut, String stdErr) {
    this.resultType = result.getResultType();
    this.result = result.getResult();
    this.endTime = LocalDateTime.now();
    this.executionCmd = executionCmd;
    this.stdOut = stdOut;
    this.stdErr = stdErr;
    this.executionCount++;
  }
}
