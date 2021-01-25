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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import pipelite.time.Time;

public class ScheduleTest {

  @Test
  public void lifecycle() {
    String pipelineName = "test";
    String cron = "0/1 * * * * ?"; // every second

    Schedule schedule = new Schedule(pipelineName);

    // Empty schedule.

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNull();
    assertThat(schedule.getCron()).isNull();

    // Set cron.

    schedule.setCron(cron);

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNull();
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Enable.
    ZonedDateTime first = ZonedDateTime.now();

    schedule.setLaunchTime(schedule.getNextLaunchTime());

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNotNull();
    assertThat(schedule.getLaunchTime()).isAfter(first);
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Wait until the schedule is executable.

    Time.wait(Duration.ofSeconds(1));

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isTrue();
    assertThat(schedule.getLaunchTime()).isNotNull();
    assertThat(schedule.getLaunchTime()).isAfter(first);
    assertThat(schedule.getLaunchTime()).isBeforeOrEqualTo(ZonedDateTime.now());
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Disable the schedule.

    schedule.setLaunchTime(null);

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNull();
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Enable.
    ZonedDateTime second = ZonedDateTime.now();

    schedule.setLaunchTime(schedule.getNextLaunchTime());

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNotNull();
    assertThat(schedule.getLaunchTime()).isAfterOrEqualTo(second);
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Wait until the schedule is executable.

    Time.wait(Duration.ofSeconds(1));

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isTrue();
    assertThat(schedule.getLaunchTime()).isNotNull();
    assertThat(schedule.getLaunchTime()).isAfterOrEqualTo(second);
    assertThat(schedule.getLaunchTime()).isBeforeOrEqualTo(ZonedDateTime.now());
    assertThat(schedule.getCron()).isEqualTo(cron);
  }
}
