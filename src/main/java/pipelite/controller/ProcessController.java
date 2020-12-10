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
import pipelite.launcher.process.runner.ProcessRunner;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
@RequestMapping(value = "/process")
@Tag(name = "ProcessAPI", description = "Pipelite processes")
public class ProcessController {

  @Autowired Application application;
  @Autowired Environment environment;

  @GetMapping("/")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All running processes")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ProcessInfo> runningProcesses() {
    List<ProcessInfo> list = new ArrayList<>();
    application.getRunningLaunchers().stream()
        .forEach(launcher -> list.addAll(getProcesses(launcher)));
    application.getRunningSchedulers().stream()
        .forEach(launcher -> list.addAll(getProcesses(launcher)));
    getLoremIpsumProcesses(list);
    return list;
  }

  @GetMapping("/{pipelineName}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All running processes for a pipeline")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ProcessInfo> runningProcesses(
      @PathVariable(value = "pipelineName") String pipelineName) {
    List<ProcessInfo> list =
        runningProcesses().stream()
            .filter(processInfo -> pipelineName.equals(processInfo.getPipelineName()))
            .collect(Collectors.toList());
    return list;
  }

  public static List<ProcessInfo> getProcesses(ProcessRunnerPoolService service) {
    Collection<ProcessRunner> processRunners = service.getActiveProcessRunners();
    List<ProcessInfo> processes = new ArrayList<>();
    for (ProcessRunner processRunner : processRunners) {
      Duration executionTime = Duration.between(ZonedDateTime.now(), processRunner.getStartTime());
      processes.add(
          ProcessInfo.builder()
              .launcherName(service.getLauncherName())
              .pipelineName(processRunner.getPipelineName())
              .processId(processRunner.getProcessId())
              .currentExecutionStartTime(processRunner.getStartTime())
              .currentExecutionTime(
                  executionTime
                      .toString()
                      .substring(2)
                      .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                      .toLowerCase())
              .state(processRunner.getProcess().getProcessEntity().getState().name())
              .executionCount(processRunner.getProcess().getProcessEntity().getExecutionCount())
              .priority(processRunner.getProcess().getProcessEntity().getPriority())
              .build());
    }
    return processes;
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
                          .currentExecutionTime(Duration.ofMinutes(5).toString())
                          .state(lorem.getFirstNameMale())
                          .executionCount(random.nextInt(10))
                          .priority(random.nextInt(10))
                          .build()));
    }
  }
}
