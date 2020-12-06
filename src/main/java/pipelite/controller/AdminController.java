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
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Application;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;
import pipelite.launcher.process.runner.ProcessRunner;

@RestController
@RequestMapping(value = "/admin")
@Tag(name = "AdministrationAPI", description = "Administration of pipelite services")
public class AdminController {

  @Autowired Application application;

  @PutMapping("/stop")
  @ResponseStatus(HttpStatus.OK)
  @Operation(
      description = "Stop all launchers and schedulers associated with this pipelite service")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public void stop() {
    application.shutDown();
  }

  @GetMapping("/launcher")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Return all process launchers associated with this pipelite service")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<Launcher> launchers() {
    List<Launcher> list = new ArrayList<>();
    for (PipeliteLauncher launcher : getLaunchers()) {
      list.add(
          Launcher.builder()
              .launcherName(launcher.getLauncherName())
              .pipelineName(launcher.getPipelineName())
              .active(getProcesses(launcher.getActiveProcessRunners()))
              .build());
    }
    return list;
  }

  @GetMapping("/schedule")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Return all process schedules associated with this pipelite service")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<Scheduler> schedules() {
    List<Scheduler> list = new ArrayList<>();
    for (PipeliteScheduler scheduler : getSchedulers()) {
      list.add(
          Scheduler.builder()
              .schedulerName(scheduler.getLauncherName())
              .schedules(getSchedules(scheduler))
              .active(getProcesses(scheduler.getActiveProcessRunners()))
              .build());
    }
    return list;
  }

  private List<PipeliteScheduler> getSchedulers() {
    List<PipeliteScheduler> schedulers = new ArrayList<>();
    if (application.getScheduler() != null && application.getScheduler().isRunning()) {
      schedulers.add(application.getScheduler());
    }
    return schedulers;
  }

  private Collection<PipeliteLauncher> getLaunchers() {
    return application.getLaunchers().stream()
        .filter(s -> s.isRunning())
        .collect(Collectors.toList());
  }

  private List<Schedule> getSchedules(PipeliteScheduler scheduler) {
    List<Schedule> schedules = new ArrayList<>();
    scheduler
        .getActiveSchedules()
        .forEach(
            s ->
                schedules.add(
                    Schedule.builder()
                        .pipelineName(s.getScheduleEntity().getPipelineName())
                        .cron(s.getScheduleEntity().getCron())
                        .description(s.getScheduleEntity().getDescription())
                        .build()));
    return schedules;
  }

  private List<Process> getProcesses(Collection<ProcessRunner> processRunners) {
    List<Process> processes = new ArrayList<>();
    for (ProcessRunner processRunner : processRunners) {
      Duration executionTime = Duration.between(ZonedDateTime.now(), processRunner.getStartTime());
      processes.add(
          Process.builder()
              .pipelineName(processRunner.getPipelineName())
              .processId(processRunner.getProcessId())
              .currentExecutionStartTime(processRunner.getStartTime())
              .currentExecutionTime(
                  executionTime
                      .toString()
                      .substring(2)
                      .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                      .toLowerCase())
              .build());
    }
    return processes;
  }
}
