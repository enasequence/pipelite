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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;
import org.springframework.jdbc.datasource.AbstractDataSource;
import pipelite.configuration.RetryableDataSourceConfiguration;

public class DataSourceHealthCheckServiceTest {

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
    RetryableDataSourceConfiguration configuration = new RetryableDataSourceConfiguration();

    TestDataSource testDataSource = new TestDataSource();
    DataSourceHealthCheckService h =
        new DataSourceHealthCheckService(configuration, testDataSource);

    // data source is healthy

    // health check
    assertThat(h.health().getStatus()).isEqualTo(Status.UP);
    assertThat(h.isHealthy()).isTrue();
    assertThat(h.unhealthySince.get()).isNull();

    // data source throws
    testDataSource.error = true;
    ZonedDateTime since = ZonedDateTime.now();
    h.checkIfHealthy();

    // health check
    assertThat(h.health().getStatus()).isEqualTo(Status.UP);
    assertThat(h.isHealthy()).isFalse();
    assertThat(h.unhealthySince.get()).isAfterOrEqualTo(since);
    assertThat(h.unhealthySince.get()).isBeforeOrEqualTo(ZonedDateTime.now());

    // data source has been unhealthy for too long
    h.unhealthySince.set(since.minus(RetryableDataSourceConfiguration.DURATION));

    // health check
    assertThat(h.health().getStatus()).isEqualTo(Status.DOWN);
    assertThat(h.isHealthy()).isFalse();
  }
}
