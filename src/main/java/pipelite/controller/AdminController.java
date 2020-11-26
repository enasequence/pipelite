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
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;

@RestController
@RequestMapping(value = "/admin")
@Tag(name = "AdministrationAPI", description = "Administration of pipelite services")
public class AdminController {

  @Autowired(required = false)
  List<PipeliteLauncher> launchers;

  @Autowired(required = false)
  List<PipeliteScheduler> schedulers;

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
    if (launchers != null) {
      for (PipeliteLauncher launcher : launchers) {
        launcher.stopAsync();
      }
    }
    if (schedulers != null) {
      for (PipeliteScheduler scheduler : schedulers) {
        scheduler.stopAsync();
      }
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
    if (launchers != null) {
      for (PipeliteLauncher launcher : launchers) {
        list.add(
            PipeliteLauncherInfo.builder()
                .launcherName(launcher.getLauncherName())
                .pipelineName(launcher.getPipelineName())
                .stats(launcher.getStats())
                .build());
      }
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
    if (schedulers != null) {
      for (PipeliteScheduler scheduler : schedulers) {
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
    }
    return list;
  }
}
