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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import pipelite.process.ProcessState;

class ScheduleEntityTest {

  @Test
  public void lifecycle() {
    String processId = "test";
    String processId2 = "test2";
    String processId3 = "test3";
    ScheduleEntity scheduleEntity = new ScheduleEntity();

    // Start execution.

    scheduleEntity.startExecution(processId);

    assertThat(scheduleEntity.getProcessId()).isEqualTo(processId);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNull();
    assertThat(scheduleEntity.getExecutionCount()).isZero();
    assertThat(scheduleEntity.getLastCompleted()).isNull();
    assertThat(scheduleEntity.getLastFailed()).isNull();
    assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(0);
    assertThat(scheduleEntity.getStreakFailed()).isEqualTo(0);

    // End execution.

    ZonedDateTime endTime = ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), ZoneId.of("UTC"));

    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(processId);
    processEntity.setState(ProcessState.COMPLETED);
    processEntity.setEndTime(endTime);

    scheduleEntity.endExecution(processEntity);

    assertThat(scheduleEntity.getProcessId()).isEqualTo(processId);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
    assertThat(scheduleEntity.getExecutionCount()).isOne();
    assertThat(scheduleEntity.getLastCompleted()).isNotNull();
    assertThat(scheduleEntity.getLastFailed()).isNull();
    assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(1);
    assertThat(scheduleEntity.getStreakFailed()).isEqualTo(0);

    // Start 2nd execution.

    scheduleEntity.startExecution(processId2);

    assertThat(scheduleEntity.getProcessId()).isEqualTo(processId2);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNull();
    assertThat(scheduleEntity.getExecutionCount()).isOne();
    assertThat(scheduleEntity.getLastCompleted()).isNotNull();
    assertThat(scheduleEntity.getLastFailed()).isNull();
    assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(1);
    assertThat(scheduleEntity.getStreakFailed()).isEqualTo(0);

    // End 2nd execution.

    ZonedDateTime endTime2 = ZonedDateTime.of(LocalDateTime.of(2020, 2, 1, 1, 1), ZoneId.of("UTC"));

    ProcessEntity processEntity2 = new ProcessEntity();
    processEntity2.setProcessId(processId2);
    processEntity2.setState(ProcessState.COMPLETED);
    processEntity2.setEndTime(endTime2);

    scheduleEntity.endExecution(processEntity2);

    assertThat(scheduleEntity.getProcessId()).isEqualTo(processId2);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(2);
    assertThat(scheduleEntity.getLastCompleted()).isNotNull();
    assertThat(scheduleEntity.getLastFailed()).isNull();
    assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(2);
    assertThat(scheduleEntity.getStreakFailed()).isEqualTo(0);

    // Start 2nd execution.

    scheduleEntity.startExecution(processId2);

    assertThat(scheduleEntity.getProcessId()).isEqualTo(processId2);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNull();
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(2);
    assertThat(scheduleEntity.getLastCompleted()).isNotNull();
    assertThat(scheduleEntity.getLastFailed()).isNull();
    assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(2);
    assertThat(scheduleEntity.getStreakFailed()).isEqualTo(0);

    // End 3rd execution.

    ZonedDateTime endTime3 = ZonedDateTime.of(LocalDateTime.of(2020, 3, 1, 1, 1), ZoneId.of("UTC"));

    ProcessEntity processEntity3 = new ProcessEntity();
    processEntity3.setProcessId(processId3);
    processEntity3.setState(ProcessState.FAILED);
    processEntity3.setEndTime(endTime3);

    scheduleEntity.endExecution(processEntity3);

    assertThat(scheduleEntity.getProcessId()).isEqualTo(processId2);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(3);
    assertThat(scheduleEntity.getLastCompleted()).isNotNull();
    assertThat(scheduleEntity.getLastFailed()).isNotNull();
    assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(0);
    assertThat(scheduleEntity.getStreakFailed()).isEqualTo(1);
  }

  @Test
  public void isResumeProcess() {
    ScheduleEntity scheduleEntity = new ScheduleEntity();

    assertThat(scheduleEntity.isResumeProcess()).isFalse();

    scheduleEntity.setStartTime(ZonedDateTime.now());
    assertThat(scheduleEntity.isResumeProcess()).isFalse();

    scheduleEntity.setProcessId("1");
    assertThat(scheduleEntity.isResumeProcess()).isTrue();

    scheduleEntity.setEndTime(ZonedDateTime.now());
    assertThat(scheduleEntity.isResumeProcess()).isFalse();
  }
}
