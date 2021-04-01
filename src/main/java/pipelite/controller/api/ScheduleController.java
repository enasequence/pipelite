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
package pipelite.controller.api;

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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import pipelite.PipeliteApplication;
import pipelite.configuration.ServiceConfiguration;
import pipelite.controller.api.info.ScheduleInfo;
import pipelite.controller.utils.LoremUtils;
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
    getLoremIpsumSchedules(list);
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

  private void getLoremIpsumSchedules(List<ScheduleInfo> list) {
    if (LoremUtils.isActiveProfile(environment)) {
      Lorem lorem = LoremIpsum.getInstance();
      IntStream.range(1, 100)
          .forEach(
              i -> {
                String startTime = TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(1));
                String endTime = TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(2));
                String nextTime = TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(3));
                String lastCompleted =
                    TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(4));
                String lastFailed = TimeUtils.humanReadableDate(ZonedDateTime.now().minusHours(5));
                list.add(
                    ScheduleInfo.builder()
                        .serviceName(lorem.getFirstNameFemale())
                        .pipelineName(lorem.getCountry())
                        .cron(lorem.getWords(1))
                        .description(lorem.getWords(5))
                        .startTime(startTime)
                        .endTime(endTime)
                        .nextTime(nextTime)
                        .lastCompleted(lastCompleted)
                        .lastFailed(lastFailed)
                        .completedStreak(5)
                        .failedStreak(1)
                        .processId(lorem.getWords(1))
                        .build());
              });
    }
  }
}
