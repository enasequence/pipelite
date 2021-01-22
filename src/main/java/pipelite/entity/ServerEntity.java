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

import java.io.Serializable;
import javax.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@IdClass(ServerEntity.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@NamedNativeQuery(
    name = "ServerEntity.findServers",
    query = "SELECT DISTINCT HOST, PORT, CONTEXT_PATH FROM PIPELITE2_LAUNCHER_LOCK",
    resultClass = ServerEntity.class)
public class ServerEntity implements Serializable {
  @Id
  @Column(name = "HOST")
  private String host;

  @Id
  @Column(name = "PORT")
  private Integer port;

  @Column(name = "CONTEXT_PATH")
  private String contextPath;
}
