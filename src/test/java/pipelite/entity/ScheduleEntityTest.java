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
package pipelite.entity;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class ScheduleEntityTest {

  @Test
  public void isActive() {
    ScheduleEntity scheduleEntity = new ScheduleEntity();

    assertThat(scheduleEntity.isActive()).isFalse();

    scheduleEntity.setStartTime(ZonedDateTime.now());
    assertThat(scheduleEntity.isActive()).isFalse();

    scheduleEntity.setProcessId("1");
    assertThat(scheduleEntity.isActive()).isTrue();

    scheduleEntity.setEndTime(ZonedDateTime.now());
    assertThat(scheduleEntity.isActive()).isFalse();
  }
}
