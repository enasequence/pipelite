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
package pipelite.controller;

import static pipelite.metrics.TimeSeriesMetrics.*;

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Application;
import pipelite.controller.info.PipelineInfo;
import pipelite.controller.info.PipelineStatusInfo;
import pipelite.controller.utils.LoremUtils;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.process.runner.ProcessRunnerResult;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.ProcessState;
import pipelite.service.ProcessService;
import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.components.Figure;

@RestController
@RequestMapping(value = "/pipeline")
@Tag(name = "PipelineAPI", description = "Pipelines")
public class PipelineController {

  @Autowired Application application;
  @Autowired Environment environment;
  @Autowired ProcessService processService;
  @Autowired PipeliteMetrics pipeliteMetrics;

  private static final int LOREM_IPSUM_PROCESSES = 5;
  private static final ZonedDateTime LOREM_IPSUM_SINCE =
      ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 0), ZoneId.of("UTC"));

  @GetMapping("/local")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Pipelines running in this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public Collection<PipelineInfo> localPipelines() {
    List<PipelineInfo> list = getLocalPipelines(application.getRunningLaunchers());
    getLoremIpsumLocalPipelines(list);
    return list;
  }

  public static List<PipelineInfo> getLocalPipelines(
      Collection<PipeliteLauncher> pipeliteLaunchers) {
    List<PipelineInfo> pipelines = new ArrayList<>();
    for (PipeliteLauncher pipeliteLauncher : pipeliteLaunchers) {
      pipelines.add(
          PipelineInfo.builder()
              .pipelineName(pipeliteLauncher.getPipelineName())
              .maxRunningProcessCount(pipeliteLauncher.getProcessParallelism())
              .runningProcessCount(pipeliteLauncher.getActiveProcessCount())
              .build());
    }
    return pipelines;
  }

  private void getLoremIpsumLocalPipelines(List<PipelineInfo> list) {
    if (LoremUtils.isActiveProfile(environment)) {
      Random random = new Random();
      Lorem lorem = LoremIpsum.getInstance();
      for (int i = 0; i < LOREM_IPSUM_PROCESSES; ++i) {
        list.add(
            PipelineInfo.builder()
                .pipelineName(lorem.getCountry())
                .maxRunningProcessCount(random.nextInt(50))
                .runningProcessCount(random.nextInt(10))
                .build());
      }
    }
  }

  @GetMapping("/local/status")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Status for pipelines running in this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<PipelineStatusInfo> localPipelinesStates() {
    List<PipelineStatusInfo> list = getLocalPipelinesStates(application.getRunningLaunchers());
    getLoremIpsumLocalPipelineStates(list);
    return list;
  }

  public List<PipelineStatusInfo> getLocalPipelinesStates(
      Collection<PipeliteLauncher> pipeliteLaunchers) {
    List<PipelineStatusInfo> list = new ArrayList<>();
    processService.getProcessStateSummary().stream()
        .filter(
            s ->
                pipeliteLaunchers.stream()
                    .anyMatch(v -> v.getPipelineName().equals(s.getPipelineName())))
        .forEach(
            s ->
                list.add(
                    PipelineStatusInfo.builder()
                        .pipelineName(s.getPipelineName())
                        .pendingCount(s.getPendingCount())
                        .activeCount(s.getActiveCount())
                        .completedCount(s.getCompletedCount())
                        .failedCount(s.getFailedCount())
                        .build()));
    return list;
  }

  public void getLoremIpsumLocalPipelineStates(List<PipelineStatusInfo> list) {
    Random random = new Random();
    Lorem lorem = LoremIpsum.getInstance();
    for (int i = 0; i < LOREM_IPSUM_PROCESSES; ++i) {
      list.add(
          PipelineStatusInfo.builder()
              .pipelineName(lorem.getCountry())
              .pendingCount((long) random.nextInt(10))
              .activeCount((long) random.nextInt(10))
              .completedCount((long) random.nextInt(10))
              .failedCount((long) random.nextInt(10))
              .build());
    }
  }

  @GetMapping("/local/plot/history")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "History plot for pipelines running in this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public String localHistoryPlot(
      @RequestParam int since, @RequestParam String type, @RequestParam String id) {
    Duration duration = Duration.ofMinutes(since);
    if (LoremUtils.isActiveProfile(environment)) {
      return getLoremIpsumLocalHistoryPlot(duration, type, id);
    } else {
      Collection<PipeliteLauncher> pipeliteLaunchers = application.getRunningLaunchers();
      return getLocalHistoryPlot(pipeliteLaunchers, duration, type, id);
    }
  }

  public void addLocalHistoryTable(
      PipelineMetrics metrics, Collection<Table> tables, ZonedDateTime since, String type) {
    switch (type) {
      case "running":
        tables.add(getTimeSeries(metrics.process().getRunningTimeSeries(), since));
        break;
      case "completed":
        tables.add(getTimeSeries(metrics.process().getCompletedTimeSeries(), since));
        break;
      case "failed":
        tables.add(getTimeSeries(metrics.process().getFailedTimeSeries(), since));
        break;
      case "error":
        tables.add(getTimeSeries(metrics.getInternalErrorTimeSeries(), since));
        break;
    }
  }

  private String getLocalHistoryPlot(
      Collection<PipeliteLauncher> pipeliteLaunchers, Duration duration, String type, String id) {
    Collection<Table> tables = new ArrayList<>();
    ZonedDateTime since = ZonedDateTime.now().minus(duration);

    for (PipeliteLauncher pipeliteLauncher : pipeliteLaunchers) {
      PipelineMetrics metrics = pipeliteMetrics.pipeline(pipeliteLauncher.getPipelineName());
      addLocalHistoryTable(metrics, tables, since, type);
    }

    Figure figure = TimeSeriesMetrics.getPlot("", tables);
    return TimeSeriesMetrics.getPlotJavaScript(figure, id);
  }

  private void addLoremIpsumHLocalistoryTable(
      PipelineMetrics metrics, Collection<Table> tables, String type) {
    switch (type) {
      case "running":
        tables.add(metrics.process().getRunningTimeSeries());
        break;
      case "completed":
        tables.add(metrics.process().getCompletedTimeSeries());
        break;
      case "failed":
        tables.add(metrics.process().getFailedTimeSeries());
        break;
      case "error":
        tables.add(metrics.getInternalErrorTimeSeries());
        break;
    }
  }

  private Collection<PipelineMetrics> getLoremIpsumLocalHistoryMetrics(Duration duration) {
    int timeCount = 100;
    Collection<PipelineMetrics> list = new ArrayList<>();

    ZonedDateTime since = LOREM_IPSUM_SINCE.minus(duration);
    Duration increment = duration.dividedBy(timeCount);

    for (int i = 0; i < LOREM_IPSUM_PROCESSES; ++i) {
      PipelineMetrics metrics = new PipelineMetrics("pipeline" + i, new SimpleMeterRegistry());
      for (int j = 0; j < timeCount; ++j) {
        ProcessRunnerResult r = new ProcessRunnerResult();
        r.stageSuccess();
        ZonedDateTime now = since.plus(increment.multipliedBy(j + 1));
        metrics.process().setRunningCount(i, now);
        metrics.increment(ProcessState.COMPLETED, r, now);
      }
      list.add(metrics);
    }
    return list;
  }

  private String getLoremIpsumLocalHistoryPlot(Duration duration, String type, String id) {
    Collection<Table> tables = new ArrayList<>();
    for (PipelineMetrics metrics : getLoremIpsumLocalHistoryMetrics(duration)) {
      addLoremIpsumHLocalistoryTable(metrics, tables, type);
    }
    Figure figure = TimeSeriesMetrics.getPlot("", tables);
    return TimeSeriesMetrics.getPlotJavaScript(figure, id);
  }
}
