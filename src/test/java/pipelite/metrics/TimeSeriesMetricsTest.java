package pipelite.metrics;

import org.junit.jupiter.api.Test;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TimeSeriesMetricsTest {

  private static final ZonedDateTime SINCE =
      ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 0), ZoneId.of("UTC"));

  @Test
  public void getTimeSeriesWindow() {
    int minutes = 5;
    assertThat(SINCE.with(TimeSeriesMetrics.getTimeSeriesWindow(minutes))).isEqualTo(SINCE);
    assertThat(SINCE.plusMinutes(1).with(TimeSeriesMetrics.getTimeSeriesWindow(minutes)))
        .isEqualTo(SINCE.plusMinutes(minutes));
    assertThat(SINCE.plusMinutes(2).with(TimeSeriesMetrics.getTimeSeriesWindow(minutes)))
        .isEqualTo(SINCE.plusMinutes(minutes));
    assertThat(SINCE.plusMinutes(3).with(TimeSeriesMetrics.getTimeSeriesWindow(minutes)))
        .isEqualTo(SINCE.plusMinutes(minutes));
    assertThat(SINCE.plusMinutes(4).with(TimeSeriesMetrics.getTimeSeriesWindow(minutes)))
        .isEqualTo(SINCE.plusMinutes(minutes));
    assertThat(SINCE.plusMinutes(minutes).with(TimeSeriesMetrics.getTimeSeriesWindow(minutes)))
        .isEqualTo(SINCE.plusMinutes(minutes));
    assertThat(
            SINCE
                .plusMinutes(minutes)
                .plusSeconds(1)
                .with(TimeSeriesMetrics.getTimeSeriesWindow(minutes)))
        .isEqualTo(SINCE.plusMinutes(minutes * 2));
    assertThat(
            SINCE.plusMinutes(minutes * 100).with(TimeSeriesMetrics.getTimeSeriesWindow(minutes)))
        .isEqualTo(SINCE.plusMinutes(minutes * 100));
    assertThat(
            SINCE
                .plusMinutes(minutes * 100)
                .plusSeconds(1)
                .with(TimeSeriesMetrics.getTimeSeriesWindow(minutes)))
        .isEqualTo(SINCE.plusMinutes(minutes * 101));
  }
}
