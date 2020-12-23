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
import pipelite.launcher.process.runner.ProcessRunnerType;

@Entity
@Table(name = "PIPELITE_LAUNCHER_LOCK")
@Data
@NoArgsConstructor
@AllArgsConstructor
@SequenceGenerator(name = "PIPELITE_LAUNCHER_LOCK_SEQ", initialValue = 1, allocationSize = 1)
public class LauncherLockEntity {
  @Id
  @Column(name = "LAUNCHER_ID")
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "PIPELITE_LAUNCHER_LOCK_SEQ")
  private Long launcherId;

  @Column(name = "LAUNCHER_NAME", unique = true, nullable = false)
  private String launcherName;

  @Column(name = "LAUNCHER_TYPE", nullable = false)
  private ProcessRunnerType launcherType;

  @Column(name = "HOST", nullable = false)
  private String host;

  @Column(name = "PORT", nullable = false)
  private Integer port;

  @Column(name = "CONTEXT_PATH", nullable = false)
  private String contextPath;

  @Column(name = "EXPIRY", nullable = false)
  private ZonedDateTime expiry;
}
