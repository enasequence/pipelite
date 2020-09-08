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

@Entity
@Table(name = "PIPELITE_LOCK")
@IdClass(PipeliteLockId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipeliteLock {

  // TODO: change column name to LAUNCHER_NAME
  @Column(name = "ALLOCATOR_NAME")
  private String launcherName;

  // TODO: change column name to PROCESS_NAME
  @Id
  @Column(name = "PIPELINE_NAME")
  private String processName;

  @Id
  @Column(name = "LOCK_ID")
  private String lockId;
}
