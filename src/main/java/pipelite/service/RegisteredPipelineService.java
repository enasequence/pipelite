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

import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.Pipeline;
import pipelite.Schedule;
import pipelite.configuration.ServiceConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.ScheduleEntity;
import pipelite.exception.PipeliteException;

@Service
@Flogger
public class RegisteredPipelineService {

  private final Map<String, Schedule> scheduleMap = new HashMap<>();
  private final Map<String, Pipeline> pipelineMap = new HashMap<>();

  public RegisteredPipelineService(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired ScheduleService scheduleService,
      @Autowired List<Pipeline> pipelines) {
    Set<String> pipelineNames = new HashSet<>();
    for (Pipeline pipeline : pipelines) {
      String pipelineName = pipeline.getPipelineName();
      if (pipelineName == null || pipelineName.trim().isEmpty()) {
        throw new PipeliteException("Missing pipeline name");
      }
      if (pipelineNames.contains(pipelineName)) {
        throw new PipeliteException("Non-unique pipeline name: " + pipelineName);
      }
      pipelineNames.add(pipelineName);
      if (pipeline instanceof Schedule) {
        Schedule schedule = (Schedule) pipeline;
        if (!CronUtils.validate(schedule.getCron())) {
          throw new PipeliteException(
              "Invalid cron expression '"
                  + schedule.getCron()
                  + "' for pipeline schedule: "
                  + pipelineName);
        }
        scheduleMap.put(pipelineName, schedule);
      } else {
        pipelineMap.put(pipelineName, pipeline);
      }
    }

    saveSchedules(serviceConfiguration, scheduleService);
  }

  private void saveSchedules(
      ServiceConfiguration serviceConfiguration, ScheduleService scheduleService) {
    String serviceName = serviceConfiguration.getName();
    scheduleMap.values().stream()
        .forEach(
            schedule -> {
              String pipelineName = schedule.getPipelineName();
              Optional<ScheduleEntity> savedScheduleEntity =
                  scheduleService.getSavedSchedule(pipelineName);
              if (!savedScheduleEntity.isPresent()) {
                log.atInfo().log("New pipeline schedule: " + pipelineName);
                saveSchedule(serviceConfiguration, scheduleService, schedule);
              } else {
                String registeredServiceName = savedScheduleEntity.get().getServiceName();
                if (!registeredServiceName.equals(serviceName)) {
                  // TODO: allow update of service name
                  throw new PipeliteException(
                      "Service name conflict for pipeline schedule: "
                          + pipelineName
                          + ". Registered service name: "
                          + registeredServiceName
                          + ", new service name: "
                          + serviceName);
                }
                if (!savedScheduleEntity.get().getCron().equals(schedule.getCron())) {
                  log.atInfo().log("New cron for pipeline schedule: " + pipelineName);
                  saveSchedule(serviceConfiguration, scheduleService, schedule);
                }
              }
            });
  }

  private void saveSchedule(
      ServiceConfiguration serviceConfiguration,
      ScheduleService scheduleService,
      Schedule schedule) {
    log.atInfo().log("Saving pipeline schedule: " + schedule.getPipelineName());
    try {
      ScheduleEntity scheduleEntity = new ScheduleEntity();
      scheduleEntity.setCron(schedule.getCron());
      scheduleEntity.setDescription(CronUtils.describe(schedule.getCron()));
      scheduleEntity.setPipelineName(schedule.getPipelineName());
      scheduleEntity.setServiceName(serviceConfiguration.getName());
      scheduleService.saveSchedule(scheduleEntity);
    } catch (Exception ex) {
      throw new PipeliteException(
          "Failed to save pipeline schedule: " + schedule.getPipelineName(), ex);
    }
  }

  /**
   * Returns the registered pipeline names.
   *
   * @return the registered pipeline names
   */
  public List<String> getPipelineNames() {
    return pipelineMap.keySet().stream().collect(Collectors.toList());
  }

  /**
   * Returns the registered scheduled pipeline names.
   *
   * @return the registered scheduled pipeline names
   */
  public List<String> getScheduleNames() {
    return scheduleMap.keySet().stream().collect(Collectors.toList());
  }

  /**
   * Returns true if a scheduler is registered.
   *
   * @return true if a scheduler is registered.
   */
  public boolean isScheduler() {
    return !scheduleMap.isEmpty();
  }

  /**
   * Returns a registered pipeline or schedule.
   *
   * @param pipelineName the pipeline name. A pipeline is identified by its name.
   * @return the registered pipeline.
   * @throws PipeliteException if the pipeline was not found
   */
  public Pipeline getPipeline(String pipelineName) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new PipeliteException("Missing pipeline name");
    }
    if (pipelineMap.containsKey(pipelineName)) {
      return pipelineMap.get(pipelineName);
    }
    if (scheduleMap.containsKey(pipelineName)) {
      return scheduleMap.get(pipelineName);
    }
    throw new PipeliteException("Unknown pipeline: " + pipelineName);
  }
}
