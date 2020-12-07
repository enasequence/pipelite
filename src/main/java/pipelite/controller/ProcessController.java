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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Application;
import pipelite.controller.info.ProcessInfo;
import pipelite.controller.info.ProcessRunnerInfo;
import pipelite.launcher.process.runner.ProcessRunner;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

@RestController
@RequestMapping(value = "/process")
@Tag(name = "ProcessAPI", description = "Pipelite processes")
public class ProcessController {

  @Autowired Application application;

  @GetMapping("/")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All running processes")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ProcessRunnerInfo> allRunningProcesses() {
    List<ProcessRunnerInfo> list = new ArrayList<>();
    application.getRunningLaunchers().stream()
        .forEach(
            launcher ->
                list.add(
                    ProcessRunnerInfo.builder()
                        .launcherName(launcher.getLauncherName())
                        .processes(getProcesses(launcher.getActiveProcessRunners()))
                        .build()));
    application.getRunningSchedulers().stream()
        .forEach(
            scheduler ->
                list.add(
                    ProcessRunnerInfo.builder()
                        .launcherName(scheduler.getLauncherName())
                        .processes(getProcesses(scheduler.getActiveProcessRunners()))
                        .build()));
    return list;
  }

  public static List<ProcessInfo> getProcesses(Collection<ProcessRunner> processRunners) {
    List<ProcessInfo> processes = new ArrayList<>();
    for (ProcessRunner processRunner : processRunners) {
      Duration executionTime = Duration.between(ZonedDateTime.now(), processRunner.getStartTime());
      processes.add(
          ProcessInfo.builder()
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
}
