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

import com.google.common.collect.Lists;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.controller.utils.TimeUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.process.ProcessState;
import pipelite.repository.ScheduleRepository;

@Service
@Flogger
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class ScheduleService {

  private final ScheduleRepository repository;

  public ScheduleService(@Autowired ScheduleRepository repository) {
    this.repository = repository;
  }

  public List<ScheduleEntity> getSchedules() {
    return Lists.newArrayList(repository.findAll());
  }

  public List<ScheduleEntity> getSchedules(String serviceName) {
    return repository.findByServiceName(serviceName);
  }

  public Optional<ScheduleEntity> getSavedSchedule(String pipelineName) {
    return repository.findById(pipelineName);
  }

  public ScheduleEntity saveSchedule(ScheduleEntity scheduleEntity) {
    return repository.save(scheduleEntity);
  }

  public void delete(ScheduleEntity scheduleEntity) {
    repository.delete(scheduleEntity);
  }

  /**
   * Called when the next execution time is set for the schedule. Sets the next execution time and
   * saves the schedule.
   *
   * @param pipelineName the pipeline name
   * @param nextTime the next execution time
   */
  public void scheduleExecution(String pipelineName, ZonedDateTime nextTime) {
    ScheduleEntity scheduleEntity = getSavedSchedule(pipelineName).get();
    scheduleEntity.setNextTime(truncateNextTime(pipelineName, nextTime));
    saveSchedule(scheduleEntity);
  }

  /**
   * Called when the schedule execution starts. Sets the execution start time and process id.
   * Removes the execution end time and next execution time. Saves the schedule.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   */
  public void startExecution(String pipelineName, String processId) {
    log.atInfo().log("Starting scheduled process execution: " + pipelineName);
    ScheduleEntity scheduleEntity = getSavedSchedule(pipelineName).get();
    scheduleEntity.setStartTime(ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS));
    scheduleEntity.setProcessId(processId);
    scheduleEntity.setEndTime(null);
    scheduleEntity.setNextTime(null);
    saveSchedule(scheduleEntity);
  }

  /**
   * Called when the schedule execution ends. Sets the execution end, last completed and last failed
   * times. Increases the execution count and sets the completed and failed streak.
   *
   * @param processEntity the process entity
   * @param nextTime the next execution time
   */
  public void endExecution(ProcessEntity processEntity, ZonedDateTime nextTime) {
    String pipelineName = processEntity.getPipelineName();
    log.atInfo().log("Ending scheduled process execution: " + pipelineName);
    ZonedDateTime now = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    ScheduleEntity scheduleEntity = getSavedSchedule(pipelineName).get();
    scheduleEntity.setEndTime(now);
    scheduleEntity.setNextTime(truncateNextTime(pipelineName, nextTime));
    scheduleEntity.setExecutionCount(scheduleEntity.getExecutionCount() + 1);
    if (processEntity.getProcessState() == ProcessState.COMPLETED) {
      scheduleEntity.setLastCompleted(now);
      scheduleEntity.setStreakCompleted(scheduleEntity.getStreakCompleted() + 1);
      scheduleEntity.setStreakFailed(0);
    } else {
      scheduleEntity.setLastFailed(now);
      scheduleEntity.setStreakCompleted(0);
      scheduleEntity.setStreakFailed(scheduleEntity.getStreakFailed() + 1);
    }
    saveSchedule(scheduleEntity);
  }

  private ZonedDateTime truncateNextTime(String pipelineName, ZonedDateTime nextTime) {
    nextTime = nextTime.truncatedTo(ChronoUnit.SECONDS);
    log.atInfo().log(
        "Next scheduled execution time for "
            + pipelineName
            + " is "
            + TimeUtils.humanReadableDate(nextTime));
    return nextTime;
  }
}
