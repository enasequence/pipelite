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
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Application;
import pipelite.configuration.LauncherConfiguration;
import pipelite.controller.info.ScheduleInfo;
import pipelite.controller.utils.LoremUtils;
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.ScheduleEntity;
import pipelite.service.ScheduleService;

@RestController
@RequestMapping(value = "/schedule")
@Tag(name = "ScheduleAPI", description = "Schedules")
public class ScheduleController {

  @Autowired LauncherConfiguration launcherConfiguration;
  @Autowired Application application;
  @Autowired Environment environment;
  @Autowired ScheduleService scheduleService;

  @GetMapping("/run")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Schedules running in this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ScheduleInfo> runningSchedules(
      @RequestParam(required = false, defaultValue = "false") boolean relative) {
    List<ScheduleInfo> list = new ArrayList<>();
    application.getRunningSchedulers().stream()
        .forEach(scheduler -> list.addAll(getSchedules(scheduler.getLauncherName(), relative)));
    getLoremIpsumSchedules(list, relative);
    return list;
  }

  private List<ScheduleInfo> getSchedules(String schedulerName, boolean relative) {
    List<ScheduleInfo> schedules = new ArrayList<>();
    scheduleService
        .getSchedules(schedulerName)
        .forEach(s -> schedules.add(getSchedule(s, relative)));
    return schedules;
  }

  @GetMapping("/local")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Schedules managed by this server")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ScheduleInfo> localSchedules(
      @RequestParam(required = false, defaultValue = "false") boolean relative) {
    List<ScheduleInfo> list = getSchedules(launcherConfiguration.getSchedulerName(), relative);
    getLoremIpsumSchedules(list, relative);
    return list;
  }

  @GetMapping("/all")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All schedules")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ScheduleInfo> allSchedules(
      @RequestParam(required = false, defaultValue = "false") boolean relative) {
    List<ScheduleInfo> list = getAllSchedules(relative);
    getLoremIpsumSchedules(list, relative);
    return list;
  }

  private List<ScheduleInfo> getAllSchedules(boolean relative) {
    List<ScheduleInfo> schedules = new ArrayList<>();
    scheduleService.getSchedules().forEach(s -> schedules.add(getSchedule(s, relative)));
    return schedules;
  }

  private ScheduleInfo getSchedule(ScheduleEntity s, boolean relative) {
    String startTime =
        relative
            ? TimeUtils.humanReadableDuration(s.getStartTime())
            : TimeUtils.humanReadableDate(s.getStartTime());
    String endTime =
        relative
            ? TimeUtils.humanReadableDuration(s.getEndTime())
            : TimeUtils.humanReadableDate(s.getEndTime());
    String nextTimeSign = s.getNextTime().isBefore(ZonedDateTime.now()) ? "-" : "";
    String nextTime =
        relative
            ? nextTimeSign + TimeUtils.humanReadableDuration(s.getNextTime())
            : TimeUtils.humanReadableDate(s.getNextTime());
    String lastCompleted =
        relative
            ? TimeUtils.humanReadableDuration(s.getLastCompleted())
            : TimeUtils.humanReadableDate(s.getLastCompleted());
    String lastFailed =
        relative
            ? TimeUtils.humanReadableDuration(s.getLastFailed())
            : TimeUtils.humanReadableDate(s.getLastFailed());
    return ScheduleInfo.builder()
        .schedulerName(s.getSchedulerName())
        .pipelineName(s.getPipelineName())
        .cron(s.getCron())
        .description(s.getDescription())
        .summary(
            getSummary(
                s.getActive(),
                s.getStreakCompleted(),
                s.getStreakFailed(),
                s.getLastCompleted(),
                relative))
        .startTime(startTime)
        .endTime(endTime)
        .nextTime(nextTime)
        .lastCompleted(lastCompleted)
        .lastFailed(lastFailed)
        .completedStreak(s.getStreakCompleted())
        .failedStreak(s.getStreakFailed())
        .active(s.getActive())
        .processId(s.getProcessId())
        .build();
  }

  private void getLoremIpsumSchedules(List<ScheduleInfo> list, boolean relative) {
    if (LoremUtils.isActiveProfile(environment)) {
      Lorem lorem = LoremIpsum.getInstance();
      IntStream.range(1, 100)
          .forEach(
              i -> {
                String startTime =
                    relative
                        ? TimeUtils.humanReadableDuration(ZonedDateTime.now().minusHours(1))
                        : TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(1));
                String endTime =
                    relative
                        ? TimeUtils.humanReadableDuration(ZonedDateTime.now().minusHours(2))
                        : TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(2));
                String nextTime =
                    relative
                        ? TimeUtils.humanReadableDuration(ZonedDateTime.now().minusHours(3))
                        : TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(3));
                String lastCompleted =
                    relative
                        ? TimeUtils.humanReadableDuration(ZonedDateTime.now().minusHours(4))
                        : TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(4));
                String lastFailed =
                    relative
                        ? TimeUtils.humanReadableDuration(ZonedDateTime.now().minusHours(5))
                        : TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(5));
                list.add(
                    ScheduleInfo.builder()
                        .schedulerName(lorem.getFirstNameFemale())
                        .pipelineName(lorem.getCountry())
                        .cron(lorem.getWords(1))
                        .description(lorem.getWords(5))
                        .summary(
                            getSummary(true, 0, 1, ZonedDateTime.now().minusHours(1), relative))
                        .startTime(startTime)
                        .endTime(endTime)
                        .nextTime(nextTime)
                        .lastCompleted(lastCompleted)
                        .lastFailed(lastFailed)
                        .completedStreak(5)
                        .failedStreak(1)
                        .active(true)
                        .processId(lorem.getWords(1))
                        .build());
              });
    }
  }

  public static String getSummary(
      boolean active,
      int streakCompleted,
      int streakFailed,
      ZonedDateTime lastCompleted,
      boolean relative) {
    if (!active) {
      return "The schedule is inactive.";
    }
    String str = "";
    if (streakFailed > 0) {
      str += "Last " + streakFailed + " executions have failed. ";
    }
    if (streakCompleted > 0) {
      str += "Last " + streakCompleted + " executions have succeeded. ";
    }
    if (lastCompleted != null) {
      str += "Last successful execution ";
      str +=
          relative
              ? "was " + TimeUtils.humanReadableDuration(ZonedDateTime.now().minusHours(4)) + " ago"
              : TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(4));
      str += ".";
    }
    return str.trim();
  }
}
