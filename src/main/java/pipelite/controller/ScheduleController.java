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
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.launcher.PipeliteScheduler;
import pipelite.process.ProcessState;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;

@RestController
@RequestMapping(value = "/schedule")
@Tag(name = "ScheduleAPI", description = "Pipelite schedules")
public class ScheduleController {

  @Autowired Application application;
  @Autowired Environment environment;
  @Autowired ScheduleService scheduleService;
  @Autowired ProcessService processService;

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
        .forEach(scheduler -> list.addAll(getLocalSchedules(scheduler)));
    getLoremIpsumSchedules(list);
    return list;
  }

  public List<ScheduleInfo> getLocalSchedules(PipeliteScheduler scheduler) {
    List<ScheduleInfo> schedules = new ArrayList<>();
    scheduler
        .getSchedules()
        .forEach(
            s -> {
              Optional<ScheduleEntity> o = scheduleService.geSavedSchedule(s.getPipelineName());
              if (!o.isPresent()) {
                return;
              }
              ScheduleEntity e = o.get();
              String processState = null;
              if (e.getProcessId() != null) {
                Optional<ProcessEntity> processEntity =
                    processService.getSavedProcess(s.getPipelineName(), e.getProcessId());
                if (processEntity.isPresent()) {
                  processState = processEntity.get().getState().name();
                }
              }
              schedules.add(
                  ScheduleInfo.builder()
                      .schedulerName(scheduler.getLauncherName())
                      .pipelineName(e.getPipelineName())
                      .cron(e.getCron())
                      .description(e.getDescription())
                      .startTime(TimeUtils.humanReadableDate(e.getStartTime()))
                      .sinceStartTime(TimeUtils.humanReadableDuration(e.getStartTime()))
                      .endTime(TimeUtils.humanReadableDate(e.getEndTime()))
                      .sinceEndTime(TimeUtils.humanReadableDuration(e.getEndTime()))
                      .runTime(TimeUtils.humanReadableDuration(e.getStartTime()))
                      .nextStartTime(TimeUtils.humanReadableDate(s.getLaunchTime()))
                      .untilNextStartTime(TimeUtils.humanReadableDuration(s.getLaunchTime()))
                      .lastCompleted(TimeUtils.humanReadableDate(e.getLastCompleted()))
                      .sinceLastCompleted(TimeUtils.humanReadableDuration(e.getLastCompleted()))
                      .lastFailed(TimeUtils.humanReadableDate(e.getLastFailed()))
                      .sinceLastFailed(TimeUtils.humanReadableDuration(e.getLastFailed()))
                      .completedStreak(e.getStreakCompleted())
                      .failedStreak(e.getStreakFailed())
                      .processId(e.getProcessId())
                      .state(processState)
                      .build());
            });
    return schedules;
  }

  public void getLoremIpsumSchedules(List<ScheduleInfo> list) {
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(profile -> "LoremIpsum".equals(profile))) {
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
                        .runTime(humanReadableDuration)
                        .nextStartTime(humanReadableDate)
                        .untilNextStartTime(humanReadableDuration)
                        .lastCompleted(humanReadableDate)
                        .sinceLastCompleted(humanReadableDuration)
                        .lastFailed(humanReadableDate)
                        .sinceLastFailed(humanReadableDuration)
                        .completedStreak(5)
                        .failedStreak(1)
                        .processId(lorem.getWords(1))
                        .state(ProcessState.COMPLETED.name())
                        .build());
              });
    }
  }
}
