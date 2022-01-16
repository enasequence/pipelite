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
package pipelite.tester.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.tester.TestType;

public class MetricsTestAsserter {

  private MetricsTestAsserter() {}

  public static void assertCompletedMetrics(
      TestType testType, PipeliteMetrics metrics, String pipelineName, int processCnt) {

    PipelineMetrics pipelineMetrics = metrics.pipeline(pipelineName);

    // Assuming single stage in process.

    assertThat(pipelineMetrics.process().getCompletedCount())
        .isEqualTo(processCnt * testType.expectedProcessCompletedCnt());
    assertThat(pipelineMetrics.process().getFailedCount())
        .isEqualTo(processCnt * testType.expectedProcessFailedCnt());

    assertThat(pipelineMetrics.stage().getFailedCount())
        .isEqualTo(processCnt * testType.expectedStageFailedCnt());
    assertThat(pipelineMetrics.stage().getSuccessCount())
        .isEqualTo(processCnt * testType.expectedStageSuccessCnt());

    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
        .isEqualTo(processCnt * testType.expectedProcessCompletedCnt());
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
        .isEqualTo(processCnt * testType.expectedProcessFailedCnt());

    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
        .isEqualTo(processCnt * testType.expectedStageFailedCnt());
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
        .isEqualTo(processCnt * testType.expectedStageSuccessCnt());
  }
}
