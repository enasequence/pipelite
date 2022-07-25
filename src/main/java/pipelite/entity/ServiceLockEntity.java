/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "PIPELITE2_SERVICE_LOCK")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ServiceLockEntity {

  @Id
  @Column(name = "LOCK_ID", length = 36)
  private String lockId;

  @Column(name = "SERVICE_NAME", unique = true, nullable = false, length = 256)
  private String serviceName;

  @Column(name = "HOST", nullable = false, length = 256)
  private String host;

  @Column(name = "PORT", nullable = false, length = 256)
  private Integer port;

  @Column(name = "CONTEXT_PATH", nullable = false, length = 256)
  private String contextPath;

  @Column(name = "EXPIRY", nullable = false)
  private ZonedDateTime expiry;

  public static String createLockId() {
    return UUID.randomUUID().toString();
  }
}
