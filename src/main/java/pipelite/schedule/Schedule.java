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
package pipelite.schedule;

import java.time.ZonedDateTime;
import lombok.Getter;
import org.springframework.util.Assert;
import pipelite.cron.CronUtils;
import pipelite.entity.ScheduleEntity;

@Getter
public class Schedule {
  private final ScheduleEntity scheduleEntity;
  private ZonedDateTime launchTime;

  public Schedule(ScheduleEntity scheduleEntity) {
    Assert.notNull(scheduleEntity, "Missing schedule entity");
    this.scheduleEntity = scheduleEntity;
  }

  public void scheduleExecution() {
    launchTime = CronUtils.launchTime(scheduleEntity.getCron());
  }

  public void resumeExecution() {
    launchTime = scheduleEntity.getStartTime();
  }

  public void endExecution() {
    launchTime = null;
  }

  public void refreshSchedule(ScheduleEntity newScheduleEntity) {
    scheduleEntity.setCron(newScheduleEntity.getCron());
    scheduleEntity.setActive(newScheduleEntity.getActive());
  }

  public void removeSchedule() {
    scheduleEntity.setActive(false);
  }

  public ScheduleEntity getScheduleEntity() {
    return scheduleEntity;
  }

  public ZonedDateTime getLaunchTime() {
    return launchTime;
  }

  public String getPipelineName() {
    return scheduleEntity.getPipelineName();
  }

  public boolean isActive() {
    return scheduleEntity.getActive() == null || scheduleEntity.getActive();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Schedule schedule = (Schedule) o;
    return scheduleEntity.getPipelineName().equals(schedule.scheduleEntity.getPipelineName());
  }

  @Override
  public int hashCode() {
    return scheduleEntity.getPipelineName().hashCode();
  }
}
