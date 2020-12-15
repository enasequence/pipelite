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
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Application;
import pipelite.controller.info.ScheduleInfo;
import pipelite.controller.utils.TimeUtils;
import pipelite.launcher.PipeliteScheduler;

@RestController
@RequestMapping(value = "/schedule")
@Tag(name = "ScheduleAPI", description = "Pipelite schedules")
public class ScheduleController {

  @Autowired Application application;
  @Autowired Environment environment;

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

  public static List<ScheduleInfo> getLocalSchedules(PipeliteScheduler scheduler) {
    List<ScheduleInfo> schedules = new ArrayList<>();
    scheduler
        .getActiveSchedules()
        .forEach(
            s ->
                schedules.add(
                    ScheduleInfo.builder()
                        .schedulerName(scheduler.getLauncherName())
                        .pipelineName(s.getScheduleEntity().getPipelineName())
                        .cron(s.getScheduleEntity().getCron())
                        .description(s.getScheduleEntity().getDescription())
                        .lastExecution(s.getScheduleEntity().getEndTime())
                        .activeExecution(
                            (s.getScheduleEntity().getProcessId() != null)
                                ? s.getScheduleEntity().getStartTime()
                                : null)
                        .nextExecution(s.getLaunchTime())
                        .timeSinceLastExecution(
                            TimeUtils.getDurationAsStringAlwaysPositive(
                                ZonedDateTime.now(), s.getScheduleEntity().getEndTime()))
                        .currentExecutionTime(
                            (s.getScheduleEntity().getProcessId() != null)
                                ? TimeUtils.getDurationAsStringAlwaysPositive(
                                    ZonedDateTime.now(), s.getScheduleEntity().getStartTime())
                                : null)
                        .timeUntilNextExecution(
                            TimeUtils.getDurationAsStringAlwaysPositive(
                                s.getLaunchTime(), ZonedDateTime.now()))
                        .processId(s.getScheduleEntity().getProcessId())
                        .build()));
    return schedules;
  }

  public void getLoremIpsumSchedules(List<ScheduleInfo> list) {
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(profile -> "LoremIpsum".equals(profile))) {
      Lorem lorem = LoremIpsum.getInstance();
      IntStream.range(1, 100)
          .forEach(
              i ->
                  list.add(
                      ScheduleInfo.builder()
                          .schedulerName(lorem.getFirstNameFemale())
                          .pipelineName(lorem.getCountry())
                          .cron(lorem.getWords(1))
                          .description(lorem.getWords(5))
                          .lastExecution(ZonedDateTime.now())
                          .activeExecution(ZonedDateTime.now())
                          .nextExecution(ZonedDateTime.now())
                          .timeSinceLastExecution(
                              TimeUtils.getDurationAsStringAlwaysPositive(
                                  ZonedDateTime.now(), ZonedDateTime.now().plusHours(6)))
                          .currentExecutionTime(
                              TimeUtils.getDurationAsStringAlwaysPositive(
                                  ZonedDateTime.now(), ZonedDateTime.now().plusHours(1)))
                          .timeUntilNextExecution(
                              TimeUtils.getDurationAsStringAlwaysPositive(
                                  ZonedDateTime.now(), ZonedDateTime.now()))
                          .processId(lorem.getWords(1))
                          .build()));
    }
  }
}
