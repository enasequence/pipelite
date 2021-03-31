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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import pipelite.Pipeline;
import pipelite.RegisteredPipeline;
import pipelite.Schedule;
import pipelite.configuration.ServiceConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.ScheduleEntity;
import pipelite.exception.PipeliteException;

@Service
@Lazy
@Flogger
public class RegisteredPipelineService {

  private final Map<String, RegisteredPipeline> registeredPipelineMap = new HashMap<>();
  private final ServiceConfiguration serviceConfiguration;
  private final ScheduleService scheduleService;
  private final String serviceName;

  public RegisteredPipelineService(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired ScheduleService scheduleService,
      @Autowired List<RegisteredPipeline> registeredPipelines) {
    this.serviceConfiguration = serviceConfiguration;
    this.scheduleService = scheduleService;
    this.serviceName = serviceConfiguration.getName();
    for (RegisteredPipeline registeredPipeline : registeredPipelines) {
      registerPipeline(registeredPipeline);
    }
    saveSchedules();
  }

  private void registerPipeline(RegisteredPipeline registeredPipeline) {
    String pipelineName = registeredPipeline.pipelineName();
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new PipeliteException("Missing pipeline name");
    }
    if (registeredPipelineMap.containsKey(pipelineName)) {
      throw new PipeliteException("Non-unique pipeline name: " + pipelineName);
    }
    if (registeredPipeline instanceof Schedule) {
      Schedule schedule = (Schedule) registeredPipeline;
      String cron = schedule.configurePipeline().cron();
      if (cron == null) {
        throw new PipeliteException(
            "Missing cron expression for pipeline schedule: " + pipelineName);
      }
      if (!CronUtils.validate(cron)) {
        throw new PipeliteException(
            "Invalid cron expression '" + cron + "' for pipeline schedule: " + pipelineName);
      }
    } else if (registeredPipeline instanceof Pipeline) {
      Pipeline pipeline = (Pipeline) registeredPipeline;
      int pipelineParallelism = pipeline.configurePipeline().pipelineParallelism();
      if (pipelineParallelism < 1) {
        throw new PipeliteException(
            "Invalid pipeline parallelism '"
                + pipelineParallelism
                + "' for pipeline: "
                + pipelineName);
      }
    }
    registeredPipelineMap.put(pipelineName, registeredPipeline);
  }

  private Stream<Schedule> streamSchedules() {
    return registeredPipelineMap.values().stream()
        .filter(s -> s instanceof Schedule)
        .map(s -> (Schedule) s);
  }

  private void saveSchedules() {
    streamSchedules()
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

                if (isServiceNameChanged && !serviceConfiguration.isForce()) {
                  throw new PipeliteException(
                      "Forceful startup not requested. Service name changed for pipeline schedule "
                          + pipelineName
                          + " from "
                          + registeredServiceName
                          + " to "
                          + serviceName);
                } else {
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
                          CronUtils.launchTime(savedScheduleEntity.getCron()));
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
      ScheduleEntity scheduleEntity = new ScheduleEntity();
      scheduleEntity.setCron(cron);
      scheduleEntity.setDescription(CronUtils.describe(cron));
      scheduleEntity.setPipelineName(schedule.pipelineName());
      scheduleEntity.setServiceName(serviceName);
      scheduleService.saveSchedule(scheduleEntity);
    } catch (Exception ex) {
      throw new PipeliteException(
          "Failed to create pipeline schedule: " + schedule.pipelineName(), ex);
    }
  }

  /**
   * Returns true if a scheduler is registered.
   *
   * @return true if a scheduler is registered.
   */
  public boolean isScheduler() {
    return streamSchedules().findAny().isPresent();
  }

  /**
   * Returns a registered pipeline.
   *
   * @param pipelineName the pipeline name.
   * @return the registered pipeline.
   * @throws PipeliteException if the pipeline was not found
   */
  public RegisteredPipeline getRegisteredPipeline(String pipelineName) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new PipeliteException("Missing pipeline name");
    }
    if (registeredPipelineMap.containsKey(pipelineName)) {
      return registeredPipelineMap.get(pipelineName);
    }
    throw new PipeliteException("Unknown pipeline: " + pipelineName);
  }

  /**
   * Returns a registered pipeline.
   *
   * @param pipelineName the pipeline name.
   * @return the registered pipeline.
   * @throws PipeliteException if the pipeline was not found
   */
  public <T extends RegisteredPipeline> T getRegisteredPipeline(String pipelineName, Class<T> cls) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new PipeliteException("Missing pipeline name");
    }
    RegisteredPipeline registeredPipeline = registeredPipelineMap.get(pipelineName);
    if (registeredPipeline != null && cls.isInstance(registeredPipeline)) {
      return (T) registeredPipelineMap.get(pipelineName);
    }
    throw new PipeliteException("Unknown pipeline: " + pipelineName);
  }

  /**
   * Returns registered pipelines.
   *
   * @return the registered pipelines.
   */
  public <T extends RegisteredPipeline> List<T> getRegisteredPipelines(Class<T> cls) {
    return registeredPipelineMap.values().stream()
        .filter(s -> cls.isInstance(s))
        .map(s -> (T) s)
        .collect(Collectors.toList());
  }
}
