/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.metrics.helper;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjuster;
import java.util.ArrayList;
import java.util.Collection;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.ScatterTrace;

public class TimeSeriesHelper {

  private static final int WINDOW_MINUTES = 1;

  private static final String COLUMN_TIME = "time";
  private static final String COLUMN_COUNT = "count";
  private static final String COLUMN_WHO = "who";

  public static Table getEmptyTimeSeries(String name) {
    return Table.create(
        name,
        DateTimeColumn.create(COLUMN_TIME),
        DoubleColumn.create(COLUMN_COUNT),
        StringColumn.create(COLUMN_WHO));
  }

  public static Table getTimeSeriesSince(Table table, ZonedDateTime since) {
    return table.where(getTimeColumn(table).isOnOrAfter(since.toLocalDateTime()));
  }

  public static DateTimeColumn getTimeColumn(Table table) {
    return table.dateTimeColumn(COLUMN_TIME);
  }

  public static DoubleColumn getCountColumn(Table table) {
    return table.doubleColumn(COLUMN_COUNT);
  }

  public static StringColumn getWhoColumn(Table table) {
    return table.stringColumn(COLUMN_WHO);
  }

  public static Double getCount(Table table) {
    return getCountColumn(table).sum();
  }

  public static Double getCount(Table table, ZonedDateTime since) {
    return getCount(table.where(getTimeColumn(table).isOnOrAfter(since.toLocalDateTime())));
  }

  public static TemporalAdjuster getTimeSeriesWindow(int windowMinutes) {
    return temporal -> {
      int minute = temporal.get(ChronoField.MINUTE_OF_HOUR);
      int nearestMinute = (int) Math.ceil(minute / windowMinutes) * windowMinutes;
      int adjustBy = nearestMinute - minute;
      if (adjustBy == 0
          && (temporal.get(ChronoField.SECOND_OF_MINUTE) > 0
              || temporal.get(ChronoField.NANO_OF_SECOND) > 0)) {
        adjustBy += windowMinutes;
      }
      return temporal
          .plus(adjustBy, ChronoUnit.MINUTES)
          .with(ChronoField.SECOND_OF_MINUTE, 0)
          .with(ChronoField.NANO_OF_SECOND, 0);
    };
  }

  /** The table must contain a constant value for the 'who' column. */
  public static void replaceTimeSeriesCount(
      Table table, double count, String who, ZonedDateTime now) {
    LocalDateTime window = now.with(getTimeSeriesWindow(WINDOW_MINUTES)).toLocalDateTime();
    DateTimeColumn timeColumn = getTimeColumn(table);
    DoubleColumn countColumn = getCountColumn(table);
    StringColumn whoColumn = getWhoColumn(table);
    synchronized (timeColumn) {
      int i = timeColumn.size() - 1;
      if (i < 0 || timeColumn.get(i).isBefore(window)) {
        timeColumn.append(window);
        countColumn.append(count);
        whoColumn.append(who);
      } else {
        countColumn.set(i, count);
      }
    }
  }

  /** The table must contain a constant value for the 'who' column. */
  public static void maximumTimeSeriesCount(
      Table table, double count, String who, ZonedDateTime now) {
    LocalDateTime window = now.with(getTimeSeriesWindow(WINDOW_MINUTES)).toLocalDateTime();
    DateTimeColumn timeColumn = getTimeColumn(table);
    DoubleColumn countColumn = getCountColumn(table);
    StringColumn whoColumn = getWhoColumn(table);
    synchronized (timeColumn) {
      int i = timeColumn.size() - 1;
      if (i < 0 || timeColumn.get(i).isBefore(window)) {
        timeColumn.append(window);
        countColumn.append(count);
        whoColumn.append(who);
      } else {
        if (countColumn.get(i) < count) {
          countColumn.set(i, count);
        }
      }
    }
  }

  /** The table must contain a constant value for the 'who' column. */
  public static void incrementTimeSeriesCount(
      Table table, double count, String who, ZonedDateTime now) {
    LocalDateTime window = now.with(getTimeSeriesWindow(WINDOW_MINUTES)).toLocalDateTime();
    DateTimeColumn timeColumn = getTimeColumn(table);
    DoubleColumn countColumn = getCountColumn(table);
    StringColumn whoColumn = getWhoColumn(table);
    synchronized (timeColumn) {
      int i = timeColumn.size() - 1;
      if (i < 0 || timeColumn.get(i).isBefore(window)) {
        timeColumn.append(window);
        countColumn.append(count);
        whoColumn.append(who);
      } else {
        countColumn.set(i, countColumn.get(i) + count);
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
    // Table plotTable = TimeSeriesHelper.getEmptyTimeSeries("plotTable");
    // tables.forEach(t -> plotTable.append(t));
    // return TimeSeriesPlot.create(title, plotTable, COLUMN_TIME, COLUMN_COUNT, COLUMN_WHO);

    // https://github.com/jtablesaw/tablesaw/issues/782

    Layout layout = Layout.builder(title, COLUMN_TIME, COLUMN_COUNT).build();

    ArrayList<ScatterTrace> traces = new ArrayList<>();
    for (Table table : tables) {
      Table t = table.sortOn(COLUMN_TIME);
      traces.add(
          ScatterTrace.builder(getTimeColumn(t), getCountColumn(t))
              .showLegend(true)
              .name(table.name())
              .mode(ScatterTrace.Mode.LINE_AND_MARKERS)
              .build());
    }
    return new Figure(layout, traces.toArray(new ScatterTrace[0]));
  }
}
