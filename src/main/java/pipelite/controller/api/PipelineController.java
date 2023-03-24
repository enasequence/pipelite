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
package pipelite.controller.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
import pipelite.metrics.PipeliteMetrics;
import pipelite.runner.pipeline.PipelineRunner;
import pipelite.runner.process.ProcessRunner;
import pipelite.service.ProcessService;
import pipelite.service.RegisteredPipelineService;
import pipelite.service.RunnerService;

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
              if (pipelineRunner != null) {
                processRunningCount = pipelineRunner.getActiveProcessCount();
                for (ProcessRunner processRunner : pipelineRunner.getActiveProcessRunners()) {
                  stageRunningCount += processRunner.getActiveStagesCount();
                }
              }
              return getPipelineInfo(
                  p.pipelineName(), maxProcessRunningCount, processRunningCount, stageRunningCount);
            })
        .collect(Collectors.toCollection(() -> list));

    return list;
  }

  private PipelineInfo getPipelineInfo(
      String pipelineName,
      int maxProcessRunningCount,
      int processRunningCount,
      int stageRunningCount) {
    // Process state summary takes a long time to construct.
    // ProcessService.ProcessStateSummary summary = summaryMap.get(pipelineName);
    return PipelineInfo.builder()
        .pipelineName(pipelineName)
        .maxProcessRunningCount(maxProcessRunningCount)
        .processRunningCount(processRunningCount)
        .stageRunningCount(stageRunningCount)
        .build();
  }
}
