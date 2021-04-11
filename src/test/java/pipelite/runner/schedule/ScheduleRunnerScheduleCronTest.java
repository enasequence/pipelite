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
package pipelite.runner.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import pipelite.cron.CronUtils;
import pipelite.time.Time;

public class ScheduleRunnerScheduleCronTest {

  @Test
  public void lifecycle() {
    String pipelineName = "test";
    String cron = "0/1 * * * * ?"; // every second

    ScheduleCron scheduleCron = new ScheduleCron(pipelineName);

    // Empty schedule.

    assertThat(scheduleCron.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleCron.isExecutable()).isFalse();
    assertThat(scheduleCron.getLaunchTime()).isNull();
    assertThat(scheduleCron.getCron()).isNull();

    // Set cron.

    scheduleCron.setCron(cron);

    assertThat(scheduleCron.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleCron.isExecutable()).isFalse();
    assertThat(scheduleCron.getLaunchTime()).isNull();
    assertThat(scheduleCron.getCron()).isEqualTo(cron);

    // Enable.
    ZonedDateTime first = ZonedDateTime.now();

    scheduleCron.setLaunchTime(CronUtils.launchTime(scheduleCron.getCron(), null));

    assertThat(scheduleCron.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleCron.isExecutable()).isFalse();
    assertThat(scheduleCron.getLaunchTime()).isNotNull();
    assertThat(scheduleCron.getLaunchTime()).isAfter(first);
    assertThat(scheduleCron.getCron()).isEqualTo(cron);

    // Wait until the schedule is executable.

    Time.wait(Duration.ofSeconds(1));

    assertThat(scheduleCron.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleCron.isExecutable()).isTrue();
    assertThat(scheduleCron.getLaunchTime()).isNotNull();
    assertThat(scheduleCron.getLaunchTime()).isAfter(first);
    assertThat(scheduleCron.getLaunchTime()).isBeforeOrEqualTo(ZonedDateTime.now());
    assertThat(scheduleCron.getCron()).isEqualTo(cron);

    // Disable the schedule.

    scheduleCron.setLaunchTime(null);

    assertThat(scheduleCron.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleCron.isExecutable()).isFalse();
    assertThat(scheduleCron.getLaunchTime()).isNull();
    assertThat(scheduleCron.getCron()).isEqualTo(cron);

    // Enable.
    ZonedDateTime second = ZonedDateTime.now();

    scheduleCron.setLaunchTime(CronUtils.launchTime(scheduleCron.getCron(), null));

    assertThat(scheduleCron.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleCron.isExecutable()).isFalse();
    assertThat(scheduleCron.getLaunchTime()).isNotNull();
    assertThat(scheduleCron.getLaunchTime()).isAfterOrEqualTo(second);
    assertThat(scheduleCron.getCron()).isEqualTo(cron);

    // Wait until the schedule is executable.

    Time.wait(Duration.ofSeconds(1));

    assertThat(scheduleCron.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleCron.isExecutable()).isTrue();
    assertThat(scheduleCron.getLaunchTime()).isNotNull();
    assertThat(scheduleCron.getLaunchTime()).isAfterOrEqualTo(second);
    assertThat(scheduleCron.getLaunchTime()).isBeforeOrEqualTo(ZonedDateTime.now());
    assertThat(scheduleCron.getCron()).isEqualTo(cron);
  }
}
