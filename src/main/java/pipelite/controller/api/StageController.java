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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.RegisteredPipeline;
import pipelite.controller.api.info.StageInfo;
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.StageEntity;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.stage.DependencyResolver;
import pipelite.service.RegisteredPipelineService;
import pipelite.service.StageService;
import pipelite.stage.Stage;

@RestController
@RequestMapping(value = "/api/stage")
@Tag(name = "StageAPI", description = "Process stages")
@Flogger
public class StageController {

  @Autowired StageService stageService;
  @Autowired RegisteredPipelineService registeredPipelineService;
  @Autowired Environment environment;

  @GetMapping("/{pipelineName}/{processId}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Process stages")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<StageInfo> stages(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "processId") String processId) {
    List<StageInfo> list = new ArrayList<>();
    AtomicReference<Process> process = new AtomicReference<>();
    try {
      RegisteredPipeline registeredPipeline =
          registeredPipelineService.getRegisteredPipeline(pipelineName);
      ProcessBuilder processBuilder = new ProcessBuilder(processId);
      registeredPipeline.configureProcess(processBuilder);
      process.set(processBuilder.build());
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log(ex.getMessage());
    }
    stageService
        .getSavedStages(pipelineName, processId)
        .forEach(stageEntity -> list.add(getStage(stageEntity, process.get())));
    return list;
  }

  public StageInfo getStage(StageEntity stageEntity, Process process) {
    String stageName = stageEntity.getStageName();
    List<String> dependsOnStageNames = new ArrayList<>();
    if (process != null) {
      Optional<Stage> stage = process.getStage(stageName);
      if (stage.isPresent()) {
        dependsOnStageNames =
            DependencyResolver.getDependsOnStagesDirectly(process, stage.get()).stream()
                .map(s -> s.getStageName())
                .collect(Collectors.toList());
      }
    }
    String executionTime = null;
    if (stageEntity.getStartTime() != null && stageEntity.getEndTime() != null) {
      executionTime =
          TimeUtils.humanReadableDuration(stageEntity.getEndTime(), stageEntity.getStartTime());
    } else if (stageEntity.getStartTime() != null) {
      executionTime =
          TimeUtils.humanReadableDuration(ZonedDateTime.now(), stageEntity.getStartTime());
    }
    return StageInfo.builder()
        .pipelineName(stageEntity.getPipelineName())
        .processId(stageEntity.getProcessId())
        .stageName(stageName)
        .stageState(stageEntity.getStageState().name())
        .errorType(stageEntity.getErrorType() != null ? stageEntity.getErrorType().name() : null)
        .startTime(TimeUtils.humanReadableDate(stageEntity.getStartTime()))
        .endTime(TimeUtils.humanReadableDate(stageEntity.getEndTime()))
        .executionTime(executionTime)
        .executionCount(stageEntity.getExecutionCount())
        .executorName(stageEntity.getExecutorName())
        .executorData(stageEntity.getExecutorData())
        .executorParams(stageEntity.getExecutorParams())
        .resultParams(stageEntity.getResultParams())
        .dependsOnStage(dependsOnStageNames)
        .build();
  }
}
