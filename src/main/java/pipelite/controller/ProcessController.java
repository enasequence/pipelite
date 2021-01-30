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
import pipelite.Application;
import pipelite.controller.info.ProcessInfo;
import pipelite.controller.utils.LoremUtils;
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.process.runner.ProcessRunner;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.process.Process;
import pipelite.service.ProcessService;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

@RestController
@RequestMapping(value = "/process")
@Tag(name = "ProcessAPI", description = "Process")
public class ProcessController {
  private static final int DEFAULT_LIMIT = 1000;

  @Autowired private Application application;
  @Autowired private Environment environment;
  @Autowired private ProcessService processService;

  @GetMapping("/{pipelineName}/{processId}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Process")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ProcessInfo> process(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "processId") String processId) {
    List<ProcessInfo> list = new ArrayList<>();
    Optional<ProcessEntity> processEntity = processService.getSavedProcess(pipelineName, processId);
    if (processEntity.isPresent()) {
      list.add(getProcess(processEntity.get()));
    }
    getLoremIpsumProcess(list);
    return list;
  }

  @GetMapping("/")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Processes running in this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ProcessInfo> processes(@RequestParam(required = false) String pipelineName) {
    List<ProcessInfo> list = new ArrayList<>();
    application
        .getRunningLaunchers()
        .forEach(launcher -> list.addAll(getProcesses(launcher, pipelineName)));
    application
        .getRunningSchedulers()
        .forEach(launcher -> list.addAll(getProcesses(launcher, pipelineName)));
    getLoremIpsumProcess(list);
    return list;
  }

  private static List<ProcessInfo> getProcesses(
      ProcessRunnerPoolService service, String pipelineName) {
    List<ProcessInfo> processes = new ArrayList<>();
    for (ProcessRunner processRunner : service.getActiveProcessRunners()) {
      Process process = processRunner.getProcess();
      ProcessEntity processEntity = process.getProcessEntity();
      if (pipelineName == null || pipelineName.equals(processRunner.getPipelineName())) {
        ProcessInfo processInfo = getProcess(processEntity);
        processes.add(processInfo);
      }
    }
    return processes;
  }

  private static ProcessInfo getProcess(ProcessEntity processEntity) {
    return ProcessInfo.builder()
        .pipelineName(processEntity.getPipelineName())
        .processId(processEntity.getProcessId())
        .state(processEntity.getProcessState().name())
        .startTime(TimeUtils.humanReadableDate(processEntity.getStartTime()))
        .endTime(TimeUtils.humanReadableDate(processEntity.getEndTime()))
        .executionCount(processEntity.getExecutionCount())
        .priority(processEntity.getPriority())
        .build();
  }

  private void getLoremIpsumProcess(List<ProcessInfo> list) {
    if (LoremUtils.isActiveProfile(environment)) {
      Lorem lorem = LoremIpsum.getInstance();
      Random random = new Random();
      list.add(
          ProcessInfo.builder()
              .pipelineName(lorem.getCountry())
              .processId(lorem.getWords(1))
              .state(lorem.getFirstNameMale())
              .startTime(TimeUtils.humanReadableDate(ZonedDateTime.now()))
              .endTime(TimeUtils.humanReadableDate(ZonedDateTime.now()))
              .executionCount(random.nextInt(10))
              .priority(random.nextInt(10))
              .build());
    }
  }
}
