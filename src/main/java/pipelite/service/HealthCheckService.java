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
package pipelite.service;

import java.sql.Connection;
import javax.sql.DataSource;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Flogger
public class HealthCheckService {

  private final DataSource dataSource;

  public HealthCheckService(@Autowired DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public boolean databaseHealthy() {
    try (Connection connection = dataSource.getConnection()) {
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unable to connect to database");
      return false;
    }
    return true;
  }
}
