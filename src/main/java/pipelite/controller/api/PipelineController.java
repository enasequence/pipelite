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

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import pipelite.controller.utils.LoremUtils;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.ProcessState;
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

  private static final int LOREM_IPSUM_PROCESSES = 5;

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
            registeredPipelineService.getRegisteredPipelines(Pipeline.class),
            runnerService.getPipelineRunners(),
            processService.getProcessStateSummary());
    getLoremIpsumPipelines(list);
    return list;
  }

  private List<PipelineInfo> getPipelines(
      Collection<Pipeline> pipelines,
      Collection<PipelineRunner> pipelineRunners,
      List<ProcessService.ProcessStateSummary> summaries) {

    Map<String, PipelineRunner> runningMap = new HashMap<>();
    pipelineRunners.forEach(s -> runningMap.put(s.getPipelineName(), s));

    Map<String, ProcessService.ProcessStateSummary> summaryMap = new HashMap<>();
    summaries.forEach(s -> summaryMap.put(s.getPipelineName(), s));

    return pipelines.stream()
        .map(
            pipeline -> {
              PipelineRunner pipelineRunner = runningMap.get(pipeline.pipelineName());
              ProcessService.ProcessStateSummary summary = summaryMap.get(pipeline.pipelineName());
              return PipelineInfo.builder()
                  .pipelineName(pipeline.pipelineName())
                  .maxRunningCount(pipeline.configurePipeline().pipelineParallelism())
                  .runningCount(pipelineRunner.getActiveProcessCount())
                  .pendingCount(summary.getPendingCount())
                  .activeCount(summary.getActiveCount())
                  .completedCount(summary.getCompletedCount())
                  .failedCount(summary.getFailedCount())
                  .build();
            })
        .collect(Collectors.toList());
  }

  private void getLoremIpsumPipelines(List<PipelineInfo> list) {
    if (LoremUtils.isActiveProfile(environment)) {
      Random random = new Random();
      Lorem lorem = LoremIpsum.getInstance();
      for (int i = 0; i < LOREM_IPSUM_PROCESSES; ++i) {
        list.add(
            PipelineInfo.builder()
                .pipelineName(lorem.getCountry())
                .maxRunningCount(random.nextInt(10))
                .runningCount(random.nextInt(10))
                .pendingCount((long) random.nextInt(10))
                .activeCount((long) random.nextInt(10))
                .completedCount((long) random.nextInt(10))
                .failedCount((long) random.nextInt(10))
                .build());
      }
    }
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
    if (LoremUtils.isActiveProfile(environment)) {
      return getLoremIpsumRunningProcessesHistoryPlot(duration, type, id);
    } else {
      return getRunningProcessesHistoryPlot(duration, type, id);
    }
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

  private void addLoremIpsumRunningProcessesHistoryTable(
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
        tables.add(metrics.process().getInternalErrorTimeSeries());
        break;
    }
  }

  private Collection<PipelineMetrics> getLoremIpsumRunningProcessesHistoryMetrics(
      Duration duration) {
    Collection<PipelineMetrics> list = new ArrayList<>();
    ZonedDateTime since = ZonedDateTime.now().minus(duration);
    int maxRunningProcesses = 100;
    int runningProcessesDurationIncrements = 100;
    Duration runningProcessesIncrement = duration.dividedBy(runningProcessesDurationIncrements);
    Duration completedProcessesMaxIncrement = duration.dividedBy(100);
    Duration failedProcessesMaxIncrement = duration.dividedBy(10);
    Duration internalErrorsMaxIncrement = duration.dividedBy(1);
    ZonedDateTime completedProcessesCurrentTime = since;
    ZonedDateTime failedProcessesCurrentTime = since;
    ZonedDateTime internalErrorsCurrentTime = since;

    Random r = new Random();
    for (int i = 0; i < LOREM_IPSUM_PROCESSES; ++i) {
      PipelineMetrics metrics = new PipelineMetrics("pipeline" + i, new SimpleMeterRegistry());
      for (int j = 0; j < runningProcessesDurationIncrements; ++j) {
        metrics
            .process()
            .setRunningProcessesCount(
                r.nextInt(maxRunningProcesses),
                since.plus(runningProcessesIncrement.multipliedBy(j + 1)));
      }

      while (completedProcessesCurrentTime.isBefore(since.plus(duration))) {
        completedProcessesCurrentTime =
            completedProcessesCurrentTime.plusSeconds(
                Math.round(completedProcessesMaxIncrement.getSeconds() * r.nextDouble()));
        metrics
            .process()
            .endProcessExecution(ProcessState.COMPLETED, completedProcessesCurrentTime);
      }
      while (failedProcessesCurrentTime.isBefore(since.plus(duration))) {
        failedProcessesCurrentTime =
            failedProcessesCurrentTime.plusSeconds(
                Math.round(failedProcessesMaxIncrement.getSeconds() * r.nextDouble()));
        metrics.process().endProcessExecution(ProcessState.FAILED, failedProcessesCurrentTime);
      }
      while (internalErrorsCurrentTime.isBefore(since.plus(duration))) {
        internalErrorsCurrentTime =
            internalErrorsCurrentTime.plusSeconds(
                Math.round(internalErrorsMaxIncrement.getSeconds() * r.nextDouble()));
        metrics.process().incrementInternalErrorCount(internalErrorsCurrentTime);
      }

      list.add(metrics);
    }
    return list;
  }

  private String getLoremIpsumRunningProcessesHistoryPlot(
      Duration duration, String type, String id) {
    Collection<Table> tables = new ArrayList<>();
    for (PipelineMetrics metrics : getLoremIpsumRunningProcessesHistoryMetrics(duration)) {
      addLoremIpsumRunningProcessesHistoryTable(metrics, tables, type);
    }
    Figure figure = TimeSeriesMetrics.getPlot("", tables);
    return TimeSeriesMetrics.getPlotJavaScript(figure, id);
  }
}
