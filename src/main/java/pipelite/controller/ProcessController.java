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
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.process.runner.ProcessRunner;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.service.ProcessService;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.IntStream;

@RestController
@RequestMapping(value = "/process")
@Tag(name = "ProcessAPI", description = "Pipelite processes")
public class ProcessController {

  private static final int DEFAULT_LIMIT = 1000;

  @Autowired private Application application;
  @Autowired private Environment environment;
  @Autowired private ProcessService processService;

  @GetMapping("/local")
  @ResponseStatus(HttpStatus.OK)
  @Operation(
      description =
          "Processes running in this service given optional pipeline name, process id and state")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ProcessInfo> localProcesses(
      @RequestParam(required = false) String pipelineName,
      @RequestParam(required = false) String processId) {
    List<ProcessInfo> list = new ArrayList<>();
    application.getRunningLaunchers().stream()
        .forEach(launcher -> list.addAll(getLocalProcesses(launcher, pipelineName, processId)));
    application.getRunningSchedulers().stream()
        .forEach(launcher -> list.addAll(getLocalProcesses(launcher, pipelineName, processId)));
    getLoremIpsumProcesses(list);
    return list;
  }

  @GetMapping("/all")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All processes given optional pipeline name, process id and state")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ProcessInfo> allProcesses(
      @RequestParam(required = false) String pipelineName,
      @RequestParam(required = false) String processId,
      @RequestParam(required = false) String state) {
    List<ProcessInfo> list = new ArrayList<>();
    processService.getProcesses(pipelineName, processId, ProcessState.valueOf(state), DEFAULT_LIMIT)
        .stream()
        .map(process -> getProcess(process))
        .forEach(process -> list.add(process));
    getLoremIpsumProcesses(list);
    return list;
  }

  public static List<ProcessInfo> getLocalProcesses(
      ProcessRunnerPoolService service, String pipelineName, String processId) {
    List<ProcessInfo> processes = new ArrayList<>();
    for (ProcessRunner processRunner : service.getActiveProcessRunners()) {
      Process process = processRunner.getProcess();
      ProcessEntity processEntity = process.getProcessEntity();
      if ((pipelineName == null || pipelineName.equals(processRunner.getPipelineName()))
          || (processId == null || processId.equals(process.getProcessId()))) {
        ZonedDateTime currentExecutionStartTime = processRunner.getStartTime();
        String currentExecutionTime =
            TimeUtils.getDurationAsStringAlwaysPositive(
                ZonedDateTime.now(), currentExecutionStartTime);
        ProcessInfo processInfo = getProcess(processEntity);
        // TODO: launcher name, execution start time and execution time is only known for processes
        // supervised by this service.
        processInfo.setLauncherName(service.getLauncherName());
        processInfo.setCurrentExecutionStartTime(currentExecutionStartTime);
        processInfo.setCurrentExecutionTime(currentExecutionTime);
        processes.add(processInfo);
      }
    }
    return processes;
  }

  public static ProcessInfo getProcess(ProcessEntity processEntity) {
    return ProcessInfo.builder()
        .pipelineName(processEntity.getPipelineName())
        .processId(processEntity.getProcessId())
        .state(processEntity.getState().name())
        .executionCount(processEntity.getExecutionCount())
        .priority(processEntity.getPriority())
        .build();
  }

  public void getLoremIpsumProcesses(List<ProcessInfo> list) {
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(profile -> "LoremIpsum".equals(profile))) {
      Lorem lorem = LoremIpsum.getInstance();
      Random random = new Random();
      IntStream.range(1, 100)
          .forEach(
              i ->
                  list.add(
                      ProcessInfo.builder()
                          .launcherName(lorem.getFirstNameFemale())
                          .pipelineName(lorem.getCountry())
                          .processId(lorem.getWords(1))
                          .currentExecutionStartTime(ZonedDateTime.now())
                          .currentExecutionTime(
                              TimeUtils.getDurationAsStringAlwaysPositive(
                                  ZonedDateTime.now(), ZonedDateTime.now().minusMinutes(5)))
                          .state(lorem.getFirstNameMale())
                          .executionCount(random.nextInt(10))
                          .priority(random.nextInt(10))
                          .build()));
    }
  }
}
