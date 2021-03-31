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
package pipelite.launcher;

import java.time.ZonedDateTime;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;

/**
 * Schedule for one pipeline. The schedule becomes executable after it has been provided with a
 * valid cron expression and has been enabled. When the schedule is enabled the cron expression is
 * evaluated and the launch time is set. The schedule should be executed once the launch time is in
 * the past. The schedule should be disabled when it is executed and enabled again after the
 * execution completes.
 */
@Flogger
public class PipeliteSchedulerSchedule {
  private final String pipelineName;
  private String cron;
  private ZonedDateTime launchTime;

  public PipeliteSchedulerSchedule(String pipelineName) {
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
   * Returns true if the schedule can be executed.
   *
   * @return true if the schedule can be executed
   */
  public boolean isExecutable() {
    return launchTime != null && !launchTime.isAfter(ZonedDateTime.now());
  }

  /**
   * Returns the current launch time.
   *
   * @return the current launch time
   */
  public ZonedDateTime getLaunchTime() {
    return launchTime;
  }

  /**
   * Sets the current launch time.
   *
   * @param launchTime the current launch time
   */
  public void setLaunchTime(ZonedDateTime launchTime) {
    this.launchTime = launchTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PipeliteSchedulerSchedule schedule = (PipeliteSchedulerSchedule) o;
    return pipelineName.equals(schedule.pipelineName);
  }

  @Override
  public int hashCode() {
    return pipelineName.hashCode();
  }
}
