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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Application;
import pipelite.controller.info.ScheduleInfo;
import pipelite.controller.utils.LoremUtils;
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.ScheduleEntity;
import pipelite.service.ScheduleService;

@RestController
@RequestMapping(value = "/schedule")
@Tag(name = "ScheduleAPI", description = "Pipelite schedules")
public class ScheduleController {

  @Autowired Application application;
  @Autowired Environment environment;
  @Autowired ScheduleService scheduleService;

  @GetMapping("/local")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Schedules running in this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ScheduleInfo> localSchedules() {
    List<ScheduleInfo> list = new ArrayList<>();
    application.getRunningSchedulers().stream()
        .forEach(scheduler -> list.addAll(getLocalSchedules(scheduler.getLauncherName())));
    getLoremIpsumSchedules(list);
    return list;
  }

  public List<ScheduleInfo> getLocalSchedules(String schedulerName) {
    List<ScheduleInfo> schedules = new ArrayList<>();
    scheduleService.getSchedules(schedulerName).forEach(s -> schedules.add(getSchedule(s)));
    return schedules;
  }

  @GetMapping("/all")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All schedules")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ScheduleInfo> allSchedules() {
    List<ScheduleInfo> list = new ArrayList<>();
    list.addAll(getAllSchedules());
    getLoremIpsumSchedules(list);
    return list;
  }

  public List<ScheduleInfo> getAllSchedules() {
    List<ScheduleInfo> schedules = new ArrayList<>();
    scheduleService.getSchedules().forEach(s -> schedules.add(getSchedule(s)));
    return schedules;
  }

  private ScheduleInfo getSchedule(ScheduleEntity s) {
    return ScheduleInfo.builder()
        .schedulerName(s.getSchedulerName())
        .pipelineName(s.getPipelineName())
        .cron(s.getCron())
        .description(s.getDescription())
        .startTime(TimeUtils.humanReadableDate(s.getStartTime()))
        .sinceStartTime(TimeUtils.humanReadableDuration(s.getStartTime()))
        .endTime(TimeUtils.humanReadableDate(s.getEndTime()))
        .sinceEndTime(TimeUtils.humanReadableDuration(s.getEndTime()))
        .nextTime(TimeUtils.humanReadableDate(s.getNextTime()))
        .untilNextTime(TimeUtils.humanReadableDuration(s.getNextTime()))
        .lastCompleted(TimeUtils.humanReadableDate(s.getLastCompleted()))
        .sinceLastCompleted(TimeUtils.humanReadableDuration(s.getLastCompleted()))
        .lastFailed(TimeUtils.humanReadableDate(s.getLastFailed()))
        .sinceLastFailed(TimeUtils.humanReadableDuration(s.getLastFailed()))
        .completedStreak(s.getStreakCompleted())
        .failedStreak(s.getStreakFailed())
        .active(s.getActive())
        .processId(s.getProcessId())
        .build();
  }

  public void getLoremIpsumSchedules(List<ScheduleInfo> list) {
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(profile -> LoremUtils.PROFILE_NAME.equals(profile))) {
      Lorem lorem = LoremIpsum.getInstance();
      IntStream.range(1, 100)
          .forEach(
              i -> {
                String humanReadableDate = TimeUtils.humanReadableDate(ZonedDateTime.now());
                String humanReadableDuration =
                    TimeUtils.humanReadableDuration(ZonedDateTime.now().minusHours(1));
                list.add(
                    ScheduleInfo.builder()
                        .schedulerName(lorem.getFirstNameFemale())
                        .pipelineName(lorem.getCountry())
                        .cron(lorem.getWords(1))
                        .description(lorem.getWords(5))
                        .startTime(humanReadableDate)
                        .sinceStartTime(humanReadableDuration)
                        .endTime(humanReadableDate)
                        .sinceEndTime(humanReadableDuration)
                        .nextTime(humanReadableDate)
                        .untilNextTime(humanReadableDuration)
                        .lastCompleted(humanReadableDate)
                        .sinceLastCompleted(humanReadableDuration)
                        .lastFailed(humanReadableDate)
                        .sinceLastFailed(humanReadableDuration)
                        .completedStreak(5)
                        .failedStreak(1)
                        .active(true)
                        .processId(lorem.getWords(1))
                        .build());
              });
    }
  }
}
