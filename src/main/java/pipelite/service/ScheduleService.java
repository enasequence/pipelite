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

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.ScheduleEntity;
import pipelite.repository.ScheduleRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class ScheduleService {

  private final ScheduleRepository repository;

  public ScheduleService(@Autowired ScheduleRepository repository) {
    this.repository = repository;
  }

  public List<ScheduleEntity> getAllProcessSchedules(String launcherName) {
    return repository.findByLauncherName(launcherName);
  }

  public ScheduleEntity saveProcessSchedule(ScheduleEntity scheduleEntity) {
    return repository.save(scheduleEntity);
  }
}