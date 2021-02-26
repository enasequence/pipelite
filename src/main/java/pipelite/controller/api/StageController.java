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

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Duration;
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
import pipelite.controller.utils.LoremUtils;
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.StageEntity;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
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
    getLoremIpsumStages(list);
    return list;
  }

  public StageInfo getStage(StageEntity stageEntity, Process process) {
    List<String> dependsOnStage = new ArrayList<>();
    if (process != null) {
      Optional<Stage> stage =
          process.getStages().stream()
              .filter(s -> s.getStageName().equals(stageEntity.getStageName()))
              .findAny();
      if (stage.isPresent()) {
        dependsOnStage =
            stage.get().getDependsOn().stream()
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
        .stageName(stageEntity.getStageName())
        .stageState(stageEntity.getStageState().name())
        .startTime(TimeUtils.humanReadableDate(stageEntity.getStartTime()))
        .endTime(TimeUtils.humanReadableDate(stageEntity.getEndTime()))
        .executionTime(executionTime)
        .executionCount(stageEntity.getExecutionCount())
        .executorName(stageEntity.getExecutorName())
        .executorData(stageEntity.getExecutorData())
        .executorParams(stageEntity.getExecutorParams())
        .resultParams(stageEntity.getResultParams())
        .dependsOnStage(dependsOnStage)
        .build();
  }

  public void getLoremIpsumStages(List<StageInfo> list) {
    if (LoremUtils.isActiveProfile(environment)) {
      Lorem lorem = LoremIpsum.getInstance();
      AtomicReference<String> previousStageName = new AtomicReference<>();
      for (int i = 0; i < 10; ++i) {
        // IntStream.range(1, 10)
        //     .forEach(
        //         i -> {
        String stageName = String.valueOf(i);
        List<String> dependsOnStages = new ArrayList<>();
        if (previousStageName.get() != null) {
          dependsOnStages.add(previousStageName.get());
        }
        previousStageName.set(String.valueOf(i));
        list.add(
            StageInfo.builder()
                .pipelineName(lorem.getCountry())
                .processId(lorem.getWords(1))
                .stageName(stageName)
                .stageState("SUCCESS")
                .startTime(TimeUtils.humanReadableDate(ZonedDateTime.now()))
                .endTime(TimeUtils.humanReadableDate(ZonedDateTime.now()))
                .executionCount(10)
                .executionTime(
                    TimeUtils.humanReadableDuration(
                        ZonedDateTime.now(), ZonedDateTime.now().minus(Duration.ofHours(1))))
                .executorName(lorem.getNameFemale())
                .executorData(lorem.getWords(1))
                .executorParams(lorem.getWords(2))
                .resultParams(lorem.getWords(2))
                .dependsOnStage(dependsOnStages)
                .build());
      }
    }
  }
}