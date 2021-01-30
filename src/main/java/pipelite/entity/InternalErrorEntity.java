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

@Entity
@Table(name = "PIPELITE2_INTERNAL_ERROR")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InternalErrorEntity {

  @Id
  @Column(name = "ERROR_ID", length = 36)
  private String errorId;

  @Column(name = "SERVICE_NAME", nullable = false, length = 256)
  private String serviceName;

  @Column(name = "PIPELINE_NAME", length = 256)
  private String pipelineName;

  @Column(name = "CLASS_NAME", nullable = false, length = 256)
  private String className;

  @Column(name = "ERROR_TIME", nullable = false)
  private ZonedDateTime errorTime;

  @Column(name = "ERROR_MESSAGE", columnDefinition = "CLOB")
  @Lob
  private String errorMessage;

  @Column(name = "ERROR_LOG", columnDefinition = "CLOB")
  @Lob
  private String errorLog;
}
