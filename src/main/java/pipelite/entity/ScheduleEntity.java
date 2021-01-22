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
import javax.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.flogger.Flogger;
import pipelite.entity.converter.BooleanConverter;

@Entity
@Table(name = "PIPELITE2_SCHEDULE")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Flogger
public class ScheduleEntity {

  /** Pipeline name. */
  @Id
  @Column(name = "PIPELINE_NAME")
  private String pipelineName;

  /** Scheduler name. */
  @Column(name = "SCHEDULER_NAME", nullable = false)
  private String schedulerName;

  /** Cron expression. */
  @Column(name = "CRON", nullable = false)
  private String cron;

  /** Cron expression description. */
  @Column(name = "DESCRIPTION")
  private String description;

  /** Is the schedule active. */
  @Column(name = "ACTIVE")
  @Convert(converter = BooleanConverter.class)
  private Boolean active;

  /** Last execution process id. */
  @Column(name = "PROCESS_ID")
  private String processId;

  /** Last execution start date. */
  @Column(name = "EXEC_START")
  private ZonedDateTime startTime;

  /** Last execution end date. */
  @Column(name = "EXEC_END")
  private ZonedDateTime endTime;

  /** Next execution time. */
  @Column(name = "EXEC_NEXT")
  private ZonedDateTime nextTime;

  /** Total execution count. */
  @Column(name = "EXEC_CNT", nullable = false)
  private int executionCount = 0;

  /** Last completed date. */
  @Column(name = "LAST_COMPLETED")
  private ZonedDateTime lastCompleted;

  /** Last failed date. */
  @Column(name = "LAST_FAILED")
  private ZonedDateTime lastFailed;

  /** Number of uninterrupted completed executions. */
  @Column(name = "STREAK_COMPLETED")
  private int streakCompleted = 0;

  /** Number of uninterrupted failed executions. */
  @Column(name = "STREAK_FAILED")
  private int streakFailed = 0;

  /**
   * Returns true if process execution can be resumed.
   *
   * @return true if process execution can be resumed
   */
  public boolean isResumeProcess() {
    return (startTime != null && endTime == null && processId != null);
  }
}
