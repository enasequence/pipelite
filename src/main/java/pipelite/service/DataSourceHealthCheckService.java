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
package pipelite.service;

import java.sql.Connection;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Service;
import pipelite.configuration.RetryableDataSourceConfiguration;
import pipelite.time.Time;

@Service
@Flogger
public class DataSourceHealthCheckService implements HealthIndicator {

  static final Duration HEALTH_CHECK_FREQUENCY = Duration.ofSeconds(5);

  private final RetryableDataSourceConfiguration configuration;
  private final DataSource dataSource;
  final AtomicReference<ZonedDateTime> unhealthySince = new AtomicReference<>();

  public DataSourceHealthCheckService(
      @Autowired RetryableDataSourceConfiguration configuration, @Autowired DataSource dataSource) {
    this.configuration = configuration;
    this.dataSource = dataSource;
    // Repeatedly check if an attempt to get a connection is successful.
    new Thread(
            () -> {
              while (true) {
                checkIfHealthy();
                Time.wait(HEALTH_CHECK_FREQUENCY);
              }
            })
        .start();
  }

  /** Checks if an attempt to get a connection is successful. */
  public void checkIfHealthy() {
    try (Connection connection = dataSource.getConnection()) {
      unhealthySince.set(null);
      // close connection
    } catch (Throwable ex) {
      log.atSevere().log(
          "Unable to get a connection from data source during health check: " + ex.getMessage());
      unhealthySince.compareAndSet(null, ZonedDateTime.now());
    }
  }

  /** Returns true if the last attempt to get a connection was successful. */
  public boolean isHealthy() {
    return unhealthySince.get() == null;
  }

  /** Returns true if pipelite is within the recovery time period for getting a connection. */
  @Override
  public Health health() {
    boolean isHealthy = configuration.isRetryable(unhealthySince.get());
    return isHealthy ? Health.up().build() : Health.down().build();
  }
}
