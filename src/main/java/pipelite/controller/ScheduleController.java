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
import pipelite.controller.info.ScheduleInfo;
import pipelite.controller.info.SchedulerInfo;
import pipelite.launcher.PipeliteScheduler;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping(value = "/schedule")
@Tag(name = "ScheduleAPI", description = "Pipelite schedules")
public class ScheduleController {

  @Autowired Application application;

  @GetMapping("/")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All schedules")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<SchedulerInfo> allSchedules() {
    List<SchedulerInfo> list = new ArrayList<>();
    application.getRunningSchedulers().stream()
        .forEach(
            scheduler ->
                list.add(
                    SchedulerInfo.builder()
                        .schedulerName(scheduler.getLauncherName())
                        .schedules(getSchedules(scheduler))
                        .processes(
                            ProcessController.getProcesses(scheduler.getActiveProcessRunners()))
                        .build()));
    return list;
  }

  public static List<ScheduleInfo> getSchedules(PipeliteScheduler scheduler) {
    List<ScheduleInfo> schedules = new ArrayList<>();
    scheduler
        .getActiveSchedules()
        .forEach(
            s ->
                schedules.add(
                    ScheduleInfo.builder()
                        .pipelineName(s.getScheduleEntity().getPipelineName())
                        .cron(s.getScheduleEntity().getCron())
                        .description(s.getScheduleEntity().getDescription())
                        .lastExecution(s.getScheduleEntity().getEndTime())
                        .activeExecution(
                            (s.getScheduleEntity().getProcessId() != null)
                                ? s.getScheduleEntity().getStartTime()
                                : null)
                        .nextExecution(s.getLaunchTime())
                        .processId(s.getScheduleEntity().getProcessId())
                        .build()));
    return schedules;
  }
}
