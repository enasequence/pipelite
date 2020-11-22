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

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class ScheduleEntityTest {

  @Test
  public void lifecycle() {
    String processId = UniqueStringGenerator.randomProcessId();
    ScheduleEntity scheduleEntity = new ScheduleEntity();

    // Start execution.

    scheduleEntity.setEndTime(LocalDateTime.now());

    scheduleEntity.startExecution(processId);

    assertThat(scheduleEntity.getProcessId()).isEqualTo(processId);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNull();
    assertThat(scheduleEntity.getExecutionCount()).isZero();

    // End execution.

    scheduleEntity.endExecution();

    assertThat(scheduleEntity.getProcessId()).isEqualTo(processId);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
    assertThat(scheduleEntity.getStartTime()).isBeforeOrEqualTo(scheduleEntity.getEndTime());
    assertThat(scheduleEntity.getExecutionCount()).isOne();
  }
}
