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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import pipelite.entity.converter.BooleanConverter;

@Entity
@Table(name = "PIPELITE_SCHEDULE")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScheduleEntity {

  @Id
  @Column(name = "PIPELINE_NAME")
  private String pipelineName;

  @Column(name = "SCHEDULER_NAME", nullable = false)
  private String schedulerName;

  @Column(name = "CRON", nullable = false)
  private String cron;

  @Column(name = "ACTIVE")
  @Convert(converter = BooleanConverter.class)
  private Boolean active;

  @Column(name = "DESCRIPTION")
  private String description;

  /* The last execution start date. */
  @Column(name = "EXEC_START")
  private ZonedDateTime startTime;

  /* The last successful execution date. */
  @Column(name = "EXEC_DATE")
  private ZonedDateTime endTime;

  @Column(name = "EXEC_CNT", nullable = false)
  private int executionCount = 0;

  @Column(name = "PROCESS_ID")
  private String processId;

  public void startExecution(String processId) {
    this.startTime = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.processId = processId;
    this.endTime = null;
  }

  public void endExecution() {
    this.startTime = null;
    this.processId = null;
    this.endTime = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.executionCount++;
  }
}
