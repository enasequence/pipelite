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
package pipelite.helper.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import pipelite.helper.TestType;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;

public class MetricsTestHelper {

  private MetricsTestHelper() {}

  public static void assertCompletedMetrics(
      TestType testType,
      PipeliteMetrics metrics,
      String pipelineName,
      int processCnt,
      int immediateRetries,
      int maximumRetries) {

    PipelineMetrics pipelineMetrics = metrics.pipeline(pipelineName);

    if (testType == TestType.PERMANENT_ERROR) {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(processCnt);
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0L);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(processCnt);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(processCnt);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(0);
    } else if (testType == TestType.NON_PERMANENT_ERROR) {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getFailedCount())
          .isIn(processCnt * (1.0 + immediateRetries), processCnt * (1.0 + maximumRetries));
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0L);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(processCnt);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isIn(processCnt * (1.0 + immediateRetries), processCnt * (1.0 + maximumRetries));
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(0);
    } else {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(processCnt);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(processCnt);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(processCnt);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(processCnt);
    }
  }
}
