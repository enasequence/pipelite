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

import java.time.Duration;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

public class RetryableDataSourceConfigurationTest {

  @Test
  public void test() {
    RetryableDataSourceConfiguration datasourceRetryConfiguration =
        new RetryableDataSourceConfiguration();

    assertThat(datasourceRetryConfiguration.getDelay())
        .isEqualTo(RetryableDataSourceConfiguration.DELAY);

    assertThat(datasourceRetryConfiguration.getDuration())
        .isEqualTo(RetryableDataSourceConfiguration.DURATION);

    assertThat(datasourceRetryConfiguration.isRetryable(ZonedDateTime.now())).isTrue();
    assertThat(
            datasourceRetryConfiguration.isRetryable(
                ZonedDateTime.now()
                    .minus(RetryableDataSourceConfiguration.DURATION)
                    .plus(Duration.ofSeconds(1))))
        .isTrue();
    assertThat(
            datasourceRetryConfiguration.isRetryable(
                ZonedDateTime.now().minus(RetryableDataSourceConfiguration.DURATION)))
        .isFalse();
  }
}
