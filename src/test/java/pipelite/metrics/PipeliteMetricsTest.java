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
package pipelite.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import pipelite.PipeliteMetricsTestFactory;

public class PipeliteMetricsTest {

  @Test
  public void internalError() {
    PipeliteMetrics metrics = PipeliteMetricsTestFactory.pipeliteMetrics();

    assertThat(metrics.getInternalErrorCount()).isZero();
    assertThat(metrics.getInternalErrorTimeSeries().rowCount()).isZero();
    assertThat(metrics.getInternalErrorTimeSeries().dateTimeColumn("time")).isNotNull();
    assertThat(metrics.getInternalErrorTimeSeries().doubleColumn("count")).isNotNull();

    metrics.incrementInternalErrorCount();

    assertThat(metrics.getInternalErrorCount()).isOne();
    assertThat(metrics.getInternalErrorTimeSeries().rowCount()).isOne();
  }
}
