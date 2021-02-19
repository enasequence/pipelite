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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Configuration;
import pipelite.service.HealthCheckService;

@Configuration
@Data
public class PipeliteHealth implements HealthIndicator {

  DataSourceRetryConfiguration dataSourceRetryConfiguration;
  HealthCheckService healthCheckService;

  public PipeliteHealth(
      @Autowired DataSourceRetryConfiguration dataSourceRetryConfiguration,
      @Autowired HealthCheckService healthCheckService) {
    this.dataSourceRetryConfiguration = dataSourceRetryConfiguration;
    this.healthCheckService = healthCheckService;
  }

  private AtomicReference<LocalDateTime> unhealthySince = new AtomicReference<>();

  @Override
  public Health health() {
    return health(LocalDateTime.now());
  }

  protected Health health(LocalDateTime now) {
    if (!healthCheckService.isDataSourceHealthy()) {
      unhealthySince.compareAndSet(null, now);
    } else {
      unhealthySince.set(null);
    }

    LocalDateTime since = unhealthySince.get();
    if (since == null
        || Duration.between(since, now)
                .abs()
                .compareTo(dataSourceRetryConfiguration.getTotalDelay())
            < 0) {
      return Health.up().build();
    } else {
      return Health.down().build();
    }
  }
}
