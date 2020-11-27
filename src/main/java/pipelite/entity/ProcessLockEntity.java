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
@Table(name = "PIPELITE_PROCESS_LOCK")
@IdClass(ProcessLockEntityId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProcessLockEntity {

  @Column(name = "LAUNCHER_ID", nullable = false)
  private Long launcherId;

  @Id
  @Column(name = "PIPELINE_NAME")
  private String pipelineName;

  @Id
  @Column(name = "PROCESS_ID")
  private String processId;
}