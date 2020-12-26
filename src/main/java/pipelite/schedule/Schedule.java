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

import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.cron.CronUtils;

/**
 * Schedule for a pipeline with cron expression and launch time. When the launch time is set the
 * schedule is executable.
 */
@Flogger
public class Schedule {
  private final String pipelineName;
  private String cron;
  private ZonedDateTime launchTime;

  public Schedule(String pipelineName) {
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.pipelineName = pipelineName;
  }

  /**
   * Returns the pipeline name.
   *
   * @return the pipeline name
   */
  public String getPipelineName() {
    return pipelineName;
  }

  /**
   * Sets the cron expression.
   *
   * @param cron the cron expression.
   */
  public void setCron(String cron) {
    this.cron = cron;
  }

  public String getCron() {
    return cron;
  }

  /**
   * Sets the launch time using the cron expression. When the launch time is set the schedule is
   * executable.
   */
  public void setLaunchTime() {
    if (cron != null) {
      launchTime = CronUtils.launchTime(cron);
    } else {
      launchTime = null;
    }
  }

  /** Removes the launch time. When the launch time is null the schedule is not executable. */
  public void removeLaunchTime() {
    launchTime = null;
  }

  /**
   * Returns the launch time.
   *
   * @return the launch time
   */
  public ZonedDateTime getLaunchTime() {
    return launchTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Schedule schedule = (Schedule) o;
    return pipelineName.equals(schedule.pipelineName);
  }

  @Override
  public int hashCode() {
    return pipelineName.hashCode();
  }
}
