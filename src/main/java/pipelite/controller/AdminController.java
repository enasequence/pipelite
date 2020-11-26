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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;

@RestController
@RequestMapping(value = "/admin")
@Tag(name = "AdministrationAPI", description = "Administration of pipelite services")
public class AdminController {

  @Autowired ApplicationContext applicationContext;

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
    ;
    for (PipeliteLauncher launcher : getLaunchers()) {
      launcher.stopAsync();
    }
    for (PipeliteScheduler scheduler : getSchedulers()) {
      scheduler.stopAsync();
    }
  }

  @GetMapping("/launcher")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Return all schedules associated with this pipelite service")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<PipeliteLauncherInfo> launchers() {
    List<PipeliteLauncherInfo> list = new ArrayList<>();
    for (PipeliteLauncher launcher : getLaunchers()) {
      list.add(
          PipeliteLauncherInfo.builder()
              .launcherName(launcher.getLauncherName())
              .pipelineName(launcher.getPipelineName())
              .stats(launcher.getStats())
              .build());
    }
    return list;
  }

  @GetMapping("/schedule")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Return all schedules associated with this pipelite service")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<PipeliteSchedulerInfo> schedules() {
    List<PipeliteSchedulerInfo> list = new ArrayList<>();
    for (PipeliteScheduler scheduler : getSchedulers()) {
      List<PipeliteSchedulerInfo.ScheduleInfo> schedules = new ArrayList<>();
      scheduler
          .getSchedules()
          .forEach(
              s ->
                  schedules.add(
                      PipeliteSchedulerInfo.ScheduleInfo.builder()
                          .pipelineName(s.getScheduleEntity().getPipelineName())
                          .cron(s.getScheduleEntity().getSchedule())
                          .description(s.getScheduleEntity().getDescription())
                          .stats(scheduler.getStats(s.getScheduleEntity().getPipelineName()))
                          .build()));
      list.add(
          PipeliteSchedulerInfo.builder()
              .schedulerName(scheduler.getSchedulerName())
              .schedules(schedules)
              .build());
    }
    return list;
  }

  private Collection<PipeliteScheduler> getSchedulers() {
    return applicationContext.getBeansOfType(PipeliteScheduler.class).values().stream()
        .filter(s -> s.isRunning())
        .collect(Collectors.toList());
  }

  private Collection<PipeliteLauncher> getLaunchers() {
    return applicationContext.getBeansOfType(PipeliteLauncher.class).values().stream()
        .filter(s -> s.isRunning())
        .collect(Collectors.toList());
  }
}
