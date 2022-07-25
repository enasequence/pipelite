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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.PipeliteApplication;
import pipelite.configuration.ServiceConfiguration;
import pipelite.controller.api.info.ScheduleInfo;
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.ScheduleEntity;
import pipelite.service.ScheduleService;

@RestController
@RequestMapping(value = "/api/schedule")
@Tag(name = "ScheduleAPI", description = "Schedules")
public class ScheduleController {

  @Autowired PipeliteApplication application;
  @Autowired Environment environment;
  @Autowired ServiceConfiguration serviceConfiguration;
  @Autowired ScheduleService scheduleService;

  @GetMapping("/")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Schedules in this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ScheduleInfo> schedules() {
    List<ScheduleInfo> list = getSchedules(serviceConfiguration.getName());
    return list;
  }

  private List<ScheduleInfo> getSchedules(String serviceName) {
    List<ScheduleInfo> schedules = new ArrayList<>();
    scheduleService.getSchedules(serviceName).forEach(s -> schedules.add(getSchedule(s)));
    return schedules;
  }

  private ScheduleInfo getSchedule(ScheduleEntity s) {
    String startTime = TimeUtils.humanReadableDate(s.getStartTime());
    String endTime = TimeUtils.humanReadableDate(s.getEndTime());
    String nextTime = null;
    if (s.getNextTime() != null) {
      nextTime = TimeUtils.humanReadableDate(s.getNextTime());
    }
    String lastCompleted = TimeUtils.humanReadableDate(s.getLastCompleted());
    String lastFailed = TimeUtils.humanReadableDate(s.getLastFailed());
    return ScheduleInfo.builder()
        .serviceName(s.getServiceName())
        .pipelineName(s.getPipelineName())
        .cron(s.getCron())
        .description(s.getDescription())
        .startTime(startTime)
        .endTime(endTime)
        .nextTime(nextTime)
        .lastCompleted(lastCompleted)
        .lastFailed(lastFailed)
        .completedStreak(s.getStreakCompleted())
        .failedStreak(s.getStreakFailed())
        .processId(s.getProcessId())
        .build();
  }
}
