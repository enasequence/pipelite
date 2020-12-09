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

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.controller.info.StageInfo;
import pipelite.entity.StageEntity;
import pipelite.service.StageService;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.IntStream;

@RestController
@RequestMapping(value = "/stage")
@Tag(name = "StageAPI", description = "Process stages")
public class StageController {

  @Autowired StageService stageService;
  @Autowired Environment environment;

  @GetMapping("/{pipelineName}/{processId}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All running stagees")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<StageInfo> stages(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "processId") String processId) {
    List<StageInfo> list = new ArrayList<>();
    stageService.getSavedStages(pipelineName, processId).stream()
        .forEach(stageEntity -> list.add(getStage(stageEntity)));
    getLoremIpsumStages(list);
    return list;
  }

  public static StageInfo getStage(StageEntity stageEntity) {
    return StageInfo.builder()
        .pipelineName(stageEntity.getPipelineName())
        .processId(stageEntity.getProcessId())
        .stageName(stageEntity.getStageName())
        .resultType(stageEntity.getResultType().name())
        .startTime(stageEntity.getStartTime())
        .endTime(stageEntity.getEndTime())
        .executionCount(stageEntity.getExecutionCount())
        .executorName(stageEntity.getExecutorName())
        .executorData(stageEntity.getExecutorData())
        .executorParams(stageEntity.getExecutorParams())
        .resultParams(stageEntity.getResultParams())
        .build();
  }

  public void getLoremIpsumStages(List<StageInfo> list) {
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(profile -> "LoremIpsum".equals(profile))) {
      Lorem lorem = LoremIpsum.getInstance();
      Random random = new Random();
      IntStream.range(1, 100)
          .forEach(
              i ->
                  list.add(
                      StageInfo.builder()
                          .pipelineName(lorem.getCountry())
                          .processId(lorem.getWords(1))
                          .stageName(lorem.getFirstNameMale())
                          .resultType(lorem.getCity())
                          .startTime(ZonedDateTime.now())
                          .endTime(ZonedDateTime.now())
                          .executionCount(10)
                          .executorName(lorem.getNameFemale())
                          .executorData(lorem.getWords(1))
                          .executorParams(lorem.getWords(2))
                          .resultParams(lorem.getWords(2))
                          .build()));
    }
  }
}
