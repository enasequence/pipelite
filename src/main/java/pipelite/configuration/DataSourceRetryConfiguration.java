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
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;

@Configuration
@ConfigurationProperties(prefix = "pipelite.datasource.retry")
@Data
public class DataSourceRetryConfiguration {

  public static final int DEFAULT_ATTEMPTS = 10;
  public static final int MIN_ATTEMPTS = 1;
  public static final Duration DEFAULT_DELAY = Duration.ofSeconds(1);
  public static final Duration MIN_DELAY = Duration.ofSeconds(1);
  public static final double DEFAULT_MULTIPLIER = 2;

  // 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 512s
  private int attempts = DEFAULT_ATTEMPTS;
  private Duration delay = DEFAULT_DELAY;
  private double multiplier = DEFAULT_MULTIPLIER;

  public int getAttempts() {
    if (attempts < MIN_ATTEMPTS) {
      return MIN_ATTEMPTS;
    }
    return attempts;
  }

  public long getDelay() {
    if (delay.compareTo(MIN_DELAY) < 0) {
      return MIN_DELAY.toMillis();
    }
    return delay.toMillis();
  }

  public long getMaxDelay() {
    if (getAttempts() == 1) {
      return getDelay();
    }
    return delay.toMillis() * (long) Math.ceil(Math.pow(multiplier, getAttempts() - 1));
  }

  public boolean recoverableException(Throwable throwable) {
    return throwable instanceof TransientDataAccessException
        || throwable instanceof RecoverableDataAccessException;
  }

  public Duration getTotalDelay() {
    long retryDuration = 0;
    for (int i = 0; i < attempts - 1; ++i) {
      retryDuration += delay.toMillis() * Math.pow(multiplier, i);
    }
    return Duration.ofMillis(retryDuration);
  }
}
