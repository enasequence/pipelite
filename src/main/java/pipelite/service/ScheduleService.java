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

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.repository.ScheduleRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class ScheduleService {

  private final ScheduleRepository repository;

  public ScheduleService(@Autowired ScheduleRepository repository) {
    this.repository = repository;
  }

  public List<ScheduleEntity> getSchedules() {
    return Lists.newArrayList(repository.findAll());
  }

  public List<ScheduleEntity> getSchedules(String schedulerName) {
    return repository.findBySchedulerName(schedulerName);
  }

  public List<ScheduleEntity> getActiveSchedules(String schedulerName) {
    return repository.findBySchedulerNameAndActive(schedulerName, true);
  }

  public Optional<ScheduleEntity> geSavedSchedule(String pipelineName) {
    return repository.findById(pipelineName);
  }

  public ScheduleEntity saveSchedule(ScheduleEntity scheduleEntity) {
    return repository.save(scheduleEntity);
  }

  public void delete(ScheduleEntity scheduleEntity) {
    repository.delete(scheduleEntity);
  }

  public void scheduleExecution(ScheduleEntity scheduleEntity, ZonedDateTime nextStart) {
    scheduleEntity.scheduleExecution(nextStart);
    saveSchedule(scheduleEntity);
  }

  public ScheduleEntity startExecution(String pipelineName, String processId) {
    ScheduleEntity scheduleEntity = geSavedSchedule(pipelineName).get();
    scheduleEntity.startExecution(processId);
    saveSchedule(scheduleEntity);
    return scheduleEntity;
  }

  public ScheduleEntity endExecution(String pipelineName, ProcessEntity processEntity) {
    ScheduleEntity scheduleEntity = geSavedSchedule(pipelineName).get();
    scheduleEntity.endExecution(processEntity);
    saveSchedule(scheduleEntity);
    return scheduleEntity;
  }
}
