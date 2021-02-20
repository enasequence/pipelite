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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pipelite.Application;
import pipelite.RegisteredPipeline;
import pipelite.controller.info.ProcessInfo;
import pipelite.controller.utils.LoremUtils;
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.process.runner.ProcessRunner;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.RegisteredPipelineService;
import pipelite.service.StageService;

@RestController
@RequestMapping(value = "/process")
@Tag(name = "ProcessAPI", description = "Process")
public class ProcessController {
  private static final int DEFAULT_LIMIT = 1000;

  @Autowired private Application application;
  @Autowired private Environment environment;
  @Autowired RegisteredPipelineService registeredPipelineService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;

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

  @Value
  @Builder
  public static class ProcessStateChangeResult {
    private final String pipelineName;
    private final String processId;
    private final boolean success;
    private final String message;
  }

  @PutMapping("/{pipelineName}/retry/{processIds}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Retry permanently failed stages")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "400", description = "Error"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public ResponseEntity<List<ProcessStateChangeResult>> retry(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "processIds") List<String> processIds) {
    List<ProcessStateChangeResult> result =
        changeProcessState(
            pipelineName, processIds, (process) -> processService.retry(pipelineName, process));
    boolean isError = result.stream().anyMatch(s -> !s.isSuccess());
    return new ResponseEntity<>(result, isError ? HttpStatus.BAD_REQUEST : HttpStatus.OK);
  }

  @PutMapping("/{pipelineName}/rerun/{stageName}/{processIds}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Rerun previously executed stages")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "400", description = "Error"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public ResponseEntity<List<ProcessStateChangeResult>> rerun(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "stageName") String stageName,
      @PathVariable(value = "processIds") List<String> processIds) {
    List<ProcessStateChangeResult> result =
        changeProcessState(
            pipelineName,
            processIds,
            (process) -> processService.rerun(pipelineName, stageName, process));
    boolean isError = result.stream().anyMatch(s -> !s.isSuccess());
    return new ResponseEntity<>(result, isError ? HttpStatus.BAD_REQUEST : HttpStatus.OK);
  }

  private List<ProcessStateChangeResult> changeProcessState(
      String pipelineName, List<String> processIds, Consumer<Process> action) {
    List<ProcessStateChangeResult> result = new ArrayList<>();

    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(pipelineName);

    for (String processId : processIds) {
      ProcessBuilder processBuilder = new ProcessBuilder(processId);
      registeredPipeline.configureProcess(processBuilder);
      Process process = processBuilder.build();
      ProcessStateChangeResult.ProcessStateChangeResultBuilder resultBuilder =
          ProcessStateChangeResult.builder().pipelineName(pipelineName).processId(processId);
      try {
        action.accept(process);
        result.add(resultBuilder.success(true).message("").build());
      } catch (Exception ex) {
        result.add(resultBuilder.success(false).message(ex.getMessage()).build());
      }
    }
    return result;
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
