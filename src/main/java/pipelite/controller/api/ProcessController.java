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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.controller.api.info.ProcessInfo;
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.ProcessEntity;
import pipelite.process.Process;
import pipelite.runner.process.ProcessRunner;
import pipelite.runner.process.ProcessRunnerPool;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;

@RestController
@RequestMapping(value = "/api/process")
@Tag(name = "ProcessAPI", description = "Process")
public class ProcessController {

  private static final int DEFAULT_MAX_PROCESS_CNT = 1000;

  @Autowired private ProcessService processService;
  @Autowired private RunnerService runnerService;

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
    runnerService
        .getPipelineRunners()
        .forEach(pipelineRunner -> list.addAll(getProcesses(pipelineRunner, pipelineName)));
    if (runnerService.isScheduleRunner()) {
      list.addAll(getProcesses(runnerService.getScheduleRunner(), pipelineName));
    }
    return list;
  }

  @GetMapping("/{pipelineName}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Processes")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ProcessInfo> process(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "maxProcessCnt", required = false) Integer maxProcessCnt) {
    if (maxProcessCnt == null) {
      maxProcessCnt = DEFAULT_MAX_PROCESS_CNT;
    }
    List<ProcessInfo> list =
        processService.getProcesses(pipelineName, null, maxProcessCnt).stream()
            .map(p -> getProcess(p))
            .collect(Collectors.toList());
    return list;
  }

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
    return list;
  }

  private static List<ProcessInfo> getProcesses(
      ProcessRunnerPool processRunnerPool, String pipelineName) {
    List<ProcessInfo> processes = new ArrayList<>();
    for (ProcessRunner processRunner : processRunnerPool.getActiveProcessRunners()) {
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
}
