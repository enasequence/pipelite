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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Entity
@Table(name = "PIPELITE_SCHEDULE")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipeliteSchedule {

  @Id
  @Column(name = "PROCESS_NAME")
  private String processName;

  @Column(name = "LAUNCHER_NAME", nullable = false)
  private String launcherName;

  @Column(name = "PROCESS_FACTORY_NAME", nullable = false)
  private String processFactoryName;

  @Column(name = "SCHEDULE", nullable = false)
  private String schedule;

  @Column(name = "DESCRIPTION")
  private String description;

  @Column(name = "EXEC_START")
  private LocalDateTime startTime;

  @Column(name = "EXEC_DATE")
  private LocalDateTime endTime;

  @Column(name = "EXEC_CNT", nullable = false)
  private int executionCount = 0;

  public void startExecution() {
    this.startTime = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.endTime = null;
  }

  public void endExecution() {
    this.endTime = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    this.executionCount++;
  }
}
