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
package pipelite.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;
import org.springframework.jdbc.datasource.AbstractDataSource;
import pipelite.service.HealthCheckService;

public class PipeliteHealthTest {

  public class TestDataSource extends AbstractDataSource {

    public boolean error = false;

    @Override
    public Connection getConnection() throws SQLException {
      if (error) {
        throw new SQLException();
      }
      return mock(Connection.class);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }
  }

  @Test
  public void test() {
    DataSourceRetryConfiguration dataSourceRetryConfiguration = new DataSourceRetryConfiguration();
    dataSourceRetryConfiguration.setAttempts(2);
    dataSourceRetryConfiguration.setDelay(Duration.ofHours(1));
    dataSourceRetryConfiguration.setMultiplier(1);

    TestDataSource testDataSource = new TestDataSource();
    HealthCheckService healthCheckService = new HealthCheckService(testDataSource);

    PipeliteHealth pipeliteHealth =
        new PipeliteHealth(dataSourceRetryConfiguration, healthCheckService);

    // up healthy check
    assertThat(pipeliteHealth.health().getStatus()).isEqualTo(Status.UP);
    assertThat(pipeliteHealth.health().getStatus()).isEqualTo(Status.UP);

    // data source throws
    testDataSource.error = true;
    LocalDateTime now = LocalDateTime.now();

    // up healthy check
    assertThat(pipeliteHealth.health(now).getStatus()).isEqualTo(Status.UP);
    assertThat(pipeliteHealth.health(now.plusMinutes(15)).getStatus()).isEqualTo(Status.UP);
    assertThat(pipeliteHealth.health(now.plusMinutes(45)).getStatus()).isEqualTo(Status.UP);
    assertThat(pipeliteHealth.health(now.plusMinutes(59).plusSeconds(59)).getStatus())
        .isEqualTo(Status.UP);

    // down healthy check
    assertThat(pipeliteHealth.health(now.plusHours(1)).getStatus()).isEqualTo(Status.DOWN);
    assertThat(pipeliteHealth.health(now.plusHours(2)).getStatus()).isEqualTo(Status.DOWN);

    // data source stops throwing
    testDataSource.error = false;

    // up healthy check
    assertThat(pipeliteHealth.health().getStatus()).isEqualTo(Status.UP);
    assertThat(pipeliteHealth.health().getStatus()).isEqualTo(Status.UP);
  }
}
