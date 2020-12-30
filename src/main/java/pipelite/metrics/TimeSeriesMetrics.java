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

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjuster;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.components.Axis;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.ScatterTrace;

public class TimeSeriesMetrics {

  private static final int WINDOW_MINUTES = 5;

  public static Table getEmptyTimeSeries(String name) {
    return Table.create(name, DateTimeColumn.create("time"), DoubleColumn.create("count"));
  }

  public static Table getTimeSeries(Table table, ZonedDateTime since) {
    return table.where(getTimeColumn(table).isOnOrAfter(since.toLocalDateTime()));
  }

  public static DateTimeColumn getTimeColumn(Table table) {
    return table.dateTimeColumn("time");
  }

  public static DoubleColumn getCountColumn(Table table) {
    return table.doubleColumn("count");
  }

  public static Double getCount(Table table) {
    return getCountColumn(table).sum();
  }

  public static Double getCount(Table table, ZonedDateTime since) {
    return getCount(table.where(getTimeColumn(table).isOnOrAfter(since.toLocalDateTime())));
  }

  public static TemporalAdjuster getTimeSeriesWindow(int minutes) {
    return temporal -> {
      int minute = temporal.get(ChronoField.MINUTE_OF_HOUR);
      int nearestMinute = (int) Math.ceil(minute / 5d) * minutes;
      int adjustBy = nearestMinute - minute;
      if (adjustBy == 0
          && (temporal.get(ChronoField.SECOND_OF_MINUTE) > 0
              || temporal.get(ChronoField.NANO_OF_SECOND) > 0)) {
        adjustBy += 5;
      }
      return temporal
          .plus(adjustBy, ChronoUnit.MINUTES)
          .with(ChronoField.SECOND_OF_MINUTE, 0)
          .with(ChronoField.NANO_OF_SECOND, 0);
    };
  }

  public static void updateCounter(Table table, double count, ZonedDateTime now) {
    if (count < 1) {
      return;
    }
    LocalDateTime window = now.with(getTimeSeriesWindow(WINDOW_MINUTES)).toLocalDateTime();
    DateTimeColumn timeColumn = getTimeColumn(table);
    DoubleColumn countColumn = getCountColumn(table);
    synchronized (timeColumn) {
      int i = timeColumn.size() - 1;
      if (i < 0 || timeColumn.get(i).isBefore(window)) {
        timeColumn.append(window);
        countColumn.append(count);
      } else {
        countColumn.set(i, countColumn.get(i) + count);
      }
    }
  }

  public static void updateGauge(Table table, double count, ZonedDateTime now) {
    LocalDateTime window = now.with(getTimeSeriesWindow(WINDOW_MINUTES)).toLocalDateTime();
    DateTimeColumn timeColumn = getTimeColumn(table);
    DoubleColumn countColumn = getCountColumn(table);
    synchronized (timeColumn) {
      int i = timeColumn.size() - 1;
      if (i < 0 || timeColumn.get(i).isBefore(window)) {
        timeColumn.append(window);
        countColumn.append(count);
      } else {
        countColumn.set(i, count);
      }
    }
  }

  public static String getPlotJavaScript(Figure figure, String id) {
    return figure
        .asJavascript(id)
        .replaceFirst("<script>", "")
        .replaceFirst("</script>", "")
        .replaceFirst(
            "Plotly\\.newPlot\\(target_" + id + ", data, layout\\)",
            "var config = {responsive: true, displayModeBar: false}; Plotly.newPlot(target_"
                + id
                + ", data, layout, config );");
  }

  public static Figure getPlot(String title, Collection<Table> tables) {
    String xTitle = "time";
    String yTitle = "count";
    Layout layout =
        Layout.builder()
            .title(title)
            .xAxis(Axis.builder().title(xTitle).build())
            .yAxis(Axis.builder().title(yTitle).build())
            .autosize(true)
            .build();

    List<ScatterTrace> traces = new ArrayList<>(tables.size());
    for (Table table : tables) {
      // table = table.sortOn("time");
      traces.add(
          ScatterTrace.builder(getTimeColumn(table), getCountColumn(table))
              .showLegend(true)
              .name(table.name())
              .mode(ScatterTrace.Mode.LINE)
              .build());
    }
    Figure figure = new Figure(layout, traces.toArray(new ScatterTrace[traces.size()]));
    return figure;
  }
}
