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

import static pipelite.metrics.helper.TimeSeriesHelper.getTimeSeriesSince;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Pipeline;
import pipelite.controller.api.info.PipelineInfo;
import pipelite.executor.AsyncExecutor;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.ProcessMetrics;
import pipelite.metrics.helper.TimeSeriesHelper;
import pipelite.runner.pipeline.PipelineRunner;
import pipelite.runner.process.ProcessRunner;
import pipelite.service.ProcessService;
import pipelite.service.RegisteredPipelineService;
import pipelite.service.RunnerService;
import pipelite.stage.Stage;
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
            runnerService, registeredPipelineService.getRegisteredPipelines(Pipeline.class));
    return list;
  }

  private List<PipelineInfo> getPipelines(
      RunnerService runnerService, Collection<Pipeline> pipelines) {

    // Process state summary takes a long time to construct.
    // List<ProcessService.ProcessStateSummary> summaries = processService.getProcessStateSummary();
    // Map<String, ProcessService.ProcessStateSummary> summaryMap = new HashMap<>();
    // summaries.forEach(s -> summaryMap.put(s.getPipelineName(), s));

    List<PipelineInfo> list = new ArrayList<>();

    // Add pipelines
    pipelines.stream()
        .map(
            p -> {
              PipelineRunner pipelineRunner =
                  runnerService.getPipelineRunner(p.pipelineName()).orElse(null);
              int maxProcessRunningCount = p.configurePipeline().pipelineParallelism();
              int processRunningCount = 0;
              int stageRunningCount = 0;
              int stageSubmitCount = 0;
              if (pipelineRunner != null) {
                processRunningCount = pipelineRunner.getActiveProcessCount();
                for (ProcessRunner processRunner : pipelineRunner.getActiveProcessRunners()) {
                  for (Stage stage : processRunner.activeStages()) {
                    stageRunningCount++;
                    if (stage.getExecutor() instanceof AsyncExecutor) {
                      String jobId = ((AsyncExecutor) stage.getExecutor()).getJobId();
                      if (jobId != null) {
                        stageSubmitCount++;
                      }
                    }
                  }
                }
              }
              return getPipelineInfo(
                  p.pipelineName(),
                  maxProcessRunningCount,
                  processRunningCount,
                  stageRunningCount,
                  stageSubmitCount);
            })
        .collect(Collectors.toCollection(() -> list));

    return list;
  }

  private PipelineInfo getPipelineInfo(
      String pipelineName,
      int maxProcessRunningCount,
      int processRunningCount,
      int stageRunningCount,
      int stageSubmitCount) {
    // Process state summary takes a long time to construct.
    // ProcessService.ProcessStateSummary summary = summaryMap.get(pipelineName);
    return PipelineInfo.builder()
        .pipelineName(pipelineName)
        .maxProcessRunningCount(maxProcessRunningCount)
        .processRunningCount(processRunningCount)
        .stageRunningCount(stageRunningCount)
        .stageSubmitCount(stageSubmitCount)
        /*
        .pendingCount(summary.getPendingCount())
        .activeCount(summary.getActiveCount())
        .completedCount(summary.getCompletedCount())
        .failedCount(summary.getFailedCount())
          */
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
              ProcessMetrics metrics = pipeliteMetrics.process(p.getPipelineName());
              addRunningProcessesHistoryTable(metrics, tables, since, type);
            });

    Figure figure = TimeSeriesHelper.getPlot("", tables);
    return TimeSeriesHelper.getPlotJavaScript(figure, id);
  }

  private void addRunningProcessesHistoryTable(
      ProcessMetrics metrics, Collection<Table> tables, ZonedDateTime since, String type) {
    switch (type) {
      case "running":
        tables.add(getTimeSeriesSince(metrics.runner().runningTimeSeries(), since));
        break;
      case "completed":
        tables.add(getTimeSeriesSince(metrics.runner().completedTimeSeries(), since));
        break;
      case "failed":
        tables.add(getTimeSeriesSince(metrics.runner().failedTimeSeries(), since));
        break;
      case "runningStages":
        tables.add(getTimeSeriesSince(metrics.runner().runningStagesTimeSeries(), since));
        break;
      case "submittedStages":
        tables.add(getTimeSeriesSince(metrics.runner().submittedStagesTimeSeries(), since));
        break;
      case "completedStages":
        tables.add(getTimeSeriesSince(metrics.runner().completedStagesTimeSeries(), since));
        break;
      case "failedStages":
        tables.add(getTimeSeriesSince(metrics.runner().failedStagesTimeSeries(), since));
        break;
    }
  }
}
