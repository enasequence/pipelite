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
package pipelite.service;

import java.util.Optional;
import javax.annotation.PostConstruct;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.Schedule;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.ScheduleEntity;
import pipelite.exception.PipeliteException;

@Service
@Flogger
public class RegisteredScheduleService {

  private final PipeliteConfiguration pipeliteConfiguration;
  private final RegisteredPipelineService registeredPipelineService;
  private final ScheduleService scheduleService;
  private final String serviceName;

  public RegisteredScheduleService(
      @Autowired PipeliteConfiguration pipeliteConfiguration,
      @Autowired RegisteredPipelineService registeredPipelineService,
      @Autowired ScheduleService scheduleService) {
    this.pipeliteConfiguration = pipeliteConfiguration;
    this.registeredPipelineService = registeredPipelineService;
    this.scheduleService = scheduleService;
    this.serviceName = pipeliteConfiguration.service().getName();
  }

  @PostConstruct
  public void saveSchedules() {
    registeredPipelineService
        .getRegisteredPipelines(Schedule.class)
        .forEach(
            schedule -> {
              String pipelineName = schedule.pipelineName();

              Optional<ScheduleEntity> savedScheduleEntityOpt =
                  scheduleService.getSavedSchedule(pipelineName);

              if (!savedScheduleEntityOpt.isPresent()) {
                createSchedule(schedule);
              } else {
                ScheduleEntity savedScheduleEntity = savedScheduleEntityOpt.get();
                String registeredCron = savedScheduleEntity.getCron();
                String registeredServiceName = savedScheduleEntity.getServiceName();
                String cron = schedule.configurePipeline().cron();
                boolean isCronChanged = !registeredCron.equals(cron);
                boolean isServiceNameChanged = !registeredServiceName.equals(serviceName);

                if (isCronChanged) {
                  log.atInfo().log(
                      "Cron changed for pipeline schedule: " + schedule.pipelineName());
                }
                if (isServiceNameChanged) {
                  log.atInfo().log(
                      "Service name changed for pipeline schedule: " + schedule.pipelineName());
                }

                if (isServiceNameChanged && !pipeliteConfiguration.service().isForce()) {
                  throw new PipeliteException(
                      "Forceful startup not requested. Service name changed for pipeline schedule "
                          + pipelineName
                          + " from "
                          + registeredServiceName
                          + " to "
                          + serviceName);
                }
                if (isServiceNameChanged) {
                  log.atWarning().log(
                      "Forceful startup requested. Changing service name for pipeline schedule "
                          + pipelineName
                          + " from "
                          + registeredServiceName
                          + " to "
                          + serviceName);
                }

                if (isCronChanged || isServiceNameChanged) {
                  log.atInfo().log("Updating pipeline schedule: " + schedule.pipelineName());
                  try {
                    savedScheduleEntity.setCron(cron);
                    savedScheduleEntity.setDescription(CronUtils.describe(cron));
                    savedScheduleEntity.setServiceName(serviceName);
                    if (!savedScheduleEntity.isActive()) {
                      savedScheduleEntity.setNextTime(
                          CronUtils.launchTime(
                              savedScheduleEntity.getCron(), savedScheduleEntity.getStartTime()));
                    }
                    scheduleService.saveSchedule(savedScheduleEntity);
                  } catch (Exception ex) {
                    throw new PipeliteException(
                        "Failed to update pipeline schedule: " + schedule.pipelineName(), ex);
                  }
                }
              }
            });
  }

  private void createSchedule(Schedule schedule) {
    log.atInfo().log("Creating pipeline schedule: " + schedule.pipelineName());
    try {
      String cron = schedule.configurePipeline().cron();
      scheduleService.createSchedule(serviceName, schedule.pipelineName(), cron);
    } catch (Exception ex) {
      throw new PipeliteException(
          "Failed to create pipeline schedule: " + schedule.pipelineName(), ex);
    }
  }
}
