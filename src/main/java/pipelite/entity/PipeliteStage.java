package pipelite.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import pipelite.executor.SerializableExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

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

  // TODO: change column name to EXEC_RESULT
  @Enumerated(EnumType.STRING)
  @Column(name = "EXEC_RESULT_TYPE", length = 15)
  private TaskExecutionResultType resultType;

  @Column(name = "EXEC_RESULT_MAP")
  @Lob
  private String resultMap;

  @Column(name = "EXEC_CNT")
  private Integer executionCount;

  @Column(name = "EXEC_START")
  private LocalDateTime startTime;

  // TODO: change column name to EXEC_END
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

  public static PipeliteStage newExecution(
      String processId, String processName, String stageName, TaskExecutor taskExecutor) {
    PipeliteStage pipeliteStage = new PipeliteStage();
    pipeliteStage.setProcessId(processId);
    pipeliteStage.setProcessName(processName);
    pipeliteStage.setStageName(stageName);
    pipeliteStage.setResultType(TaskExecutionResultType.NEW);
    pipeliteStage.setExecutionCount(0);
    if (taskExecutor instanceof SerializableExecutor) {
      pipeliteStage.setExecutorName(taskExecutor.getClass().getName());
      pipeliteStage.setExecutorData(((SerializableExecutor) taskExecutor).serialize());
    }
    return pipeliteStage;
  }

  public void retryExecution(TaskExecutor taskExecutor) {
    this.resultType = TaskExecutionResultType.ACTIVE;
    this.resultMap = null;
    this.startTime = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.endTime = null;
    this.stdOut = null;
    this.stdErr = null;
    if (taskExecutor instanceof SerializableExecutor) {
      this.executorName = taskExecutor.getClass().getName();
      this.executorData = ((SerializableExecutor) taskExecutor).serialize();
    }
  }

  public void resetExecution() {
    this.resultType = TaskExecutionResultType.NEW;
    this.resultMap = null;
    this.startTime = null;
    this.endTime = null;
    this.stdOut = null;
    this.stdErr = null;
    this.executionCount = 0;
    this.executorName = null;
    this.executorData = null;
  }

  public void endExecution(TaskExecutionResult result) {
    this.resultType = result.getResultType();
    if (result.getAttributes() != null) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        this.resultMap =
            mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result.getAttributes());
      } catch (JsonProcessingException ex) {
      }
    }
    this.endTime = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.stdOut = result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT);
    this.stdErr = result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR);
    this.executionCount++;
  }
}
