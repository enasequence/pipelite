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
package pipelite.controller.api;

import static pipelite.metrics.TimeSeriesMetrics.getTimeSeries;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Pipeline;
import pipelite.controller.api.info.PipelineInfo;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.runner.pipeline.PipelineRunner;
import pipelite.service.ProcessService;
import pipelite.service.RegisteredPipelineService;
import pipelite.service.RunnerService;
import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.components.Figure;

@RestController
@RequestMapping(value = "/api/pipeline")
@Tag(name = "PipelineAPI", description = "Pipelines")
public class PipelineController {

  @Autowired Environment environment;
  @Autowired ProcessService processService;
  @Autowired RunnerService runnerService;
  @Autowired RegisteredPipelineService registeredPipelineService;
  @Autowired PipeliteMetrics pipeliteMetrics;

  @GetMapping("/")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Pipelines running in this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public Collection<PipelineInfo> pipelines() {
    List<PipelineInfo> list =
        getPipelines(
            runnerService,
            registeredPipelineService.getRegisteredPipelines(Pipeline.class),
            processService.getProcessStateSummary());
    return list;
  }

  private List<PipelineInfo> getPipelines(
      RunnerService runnerService,
      Collection<Pipeline> pipelines,
      List<ProcessService.ProcessStateSummary> summaries) {

    Map<String, ProcessService.ProcessStateSummary> summaryMap = new HashMap<>();
    summaries.forEach(s -> summaryMap.put(s.getPipelineName(), s));

    List<PipelineInfo> list = new ArrayList<>();

    // Add pipelines
    pipelines.stream()
        .map(
            p -> {
              PipelineRunner pipelineRunner =
                  runnerService.getPipelineRunner(p.pipelineName()).orElse(null);
              int maxRunningCount = p.configurePipeline().pipelineParallelism();
              Integer runningCount =
                  pipelineRunner != null ? pipelineRunner.getActiveProcessCount() : null;
              return getPipelineInfo(summaryMap, p.pipelineName(), maxRunningCount, runningCount);
            })
        .collect(Collectors.toCollection(() -> list));

    // Add schedules
    /*
    schedules.stream()
        .filter(p -> runnerService.getScheduleRunner().isActiveSchedule(p.pipelineName()))
        .map(
            p -> {
              int maxRunningCount = 1;
              Integer runningCount = 1;
              return getPipelineInfo(summaryMap, p.pipelineName(), maxRunningCount, runningCount);
            })
        .collect(Collectors.toCollection(() -> list));
     */

    return list;
  }

  private PipelineInfo getPipelineInfo(
      Map<String, ProcessService.ProcessStateSummary> summaryMap,
      String pipelineName,
      Integer maxRunningCount,
      Integer runningCount) {
    ProcessService.ProcessStateSummary summary = summaryMap.get(pipelineName);
    return PipelineInfo.builder()
        .pipelineName(pipelineName)
        .maxRunningCount(maxRunningCount)
        .runningCount(runningCount)
        .pendingCount(summary.getPendingCount())
        .activeCount(summary.getActiveCount())
        .completedCount(summary.getCompletedCount())
        .failedCount(summary.getFailedCount())
        .build();
  }

  @GetMapping("/run/history/plot")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "History plot for pipelines running in this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public String runningProcessesHistoryPlot(
      @RequestParam int since, @RequestParam String type, @RequestParam String id) {
    Duration duration = Duration.ofMinutes(since);
    return getRunningProcessesHistoryPlot(duration, type, id);
  }

  private String getRunningProcessesHistoryPlot(Duration duration, String type, String id) {
    Collection<Table> tables = new ArrayList<>();
    ZonedDateTime since = ZonedDateTime.now().minus(duration);

    runnerService
        .getPipelineRunners()
        .forEach(
            p -> {
              PipelineMetrics metrics = pipeliteMetrics.pipeline(p.getPipelineName());
              addRunningProcessesHistoryTable(metrics, tables, since, type);
            });

    Figure figure = TimeSeriesMetrics.getPlot("", tables);
    return TimeSeriesMetrics.getPlotJavaScript(figure, id);
  }

  private void addRunningProcessesHistoryTable(
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
        tables.add(getTimeSeries(metrics.process().getInternalErrorTimeSeries(), since));
        break;
    }
  }
}
