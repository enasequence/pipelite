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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import pipelite.metrics.helper.TimeSeriesHelper;

public class TimeSeriesMetricsTest {

  private static final ZonedDateTime SINCE =
      ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 0), ZoneId.systemDefault());

  @Test
  public void getTimeSeriesWindow() {
    int windowMinutes = 1;
    assertThat(SINCE.with(TimeSeriesHelper.getTimeSeriesWindow(windowMinutes))).isEqualTo(SINCE);
    assertThat(
            SINCE
                .plusMinutes(windowMinutes)
                .with(TimeSeriesHelper.getTimeSeriesWindow(windowMinutes)))
        .isEqualTo(SINCE.plusMinutes(windowMinutes));
    assertThat(
            SINCE
                .plusMinutes(2 * windowMinutes)
                .with(TimeSeriesHelper.getTimeSeriesWindow(windowMinutes)))
        .isEqualTo(SINCE.plusMinutes(2 * windowMinutes));
    assertThat(
            SINCE
                .plusMinutes(3 * windowMinutes)
                .with(TimeSeriesHelper.getTimeSeriesWindow(windowMinutes)))
        .isEqualTo(SINCE.plusMinutes(3 * windowMinutes));
    assertThat(
            SINCE
                .plusMinutes(windowMinutes * 100)
                .with(TimeSeriesHelper.getTimeSeriesWindow(windowMinutes)))
        .isEqualTo(SINCE.plusMinutes(windowMinutes * 100));
    assertThat(
            SINCE
                .plusMinutes(windowMinutes * 100)
                .plusSeconds(1)
                .with(TimeSeriesHelper.getTimeSeriesWindow(windowMinutes)))
        .isEqualTo(SINCE.plusMinutes(windowMinutes * 101));
  }
}
