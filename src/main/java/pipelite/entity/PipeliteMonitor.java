package pipelite.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Entity
@Table(name = "PIPELITE_MONITOR")
@IdClass(PipeliteMonitorId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipeliteMonitor {

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

  @Column(name = "EXEC_START")
  private LocalDateTime startTime;

  @Column(name = "EXEC_ACTIVE")
  private LocalDateTime activeTime;

  @Column(name = "MONITOR_ACTIVE")
  private LocalDateTime monitorTime;

  @Column(name = "MONITOR_NAME")
  private String monitorName;

  @Column(name = "MONITOR_DATA")
  @Lob
  private String monitorData;
}
