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
import org.junit.jupiter.api.Test;

public class DataSourceRetryConfigurationTest {

  @Test
  public void test() {
    DataSourceRetryConfiguration datasourceRetryConfiguration = new DataSourceRetryConfiguration();
    datasourceRetryConfiguration.setAttempts(5);
    datasourceRetryConfiguration.setMultiplier(2);
    datasourceRetryConfiguration.setDelay(Duration.ofSeconds(1));

    assertThat(datasourceRetryConfiguration.getAttempts()).isEqualTo(5);
    assertThat(datasourceRetryConfiguration.getMultiplier()).isEqualTo(2);
    assertThat(datasourceRetryConfiguration.getDelay()).isEqualTo(Duration.ofSeconds(1).toMillis());

    assertThat(datasourceRetryConfiguration.getMaxDelay())
        .isEqualTo(Duration.ofSeconds(16).toMillis());
    assertThat(datasourceRetryConfiguration.getTotalDelay()).isEqualTo(Duration.ofSeconds(15));
  }
}
