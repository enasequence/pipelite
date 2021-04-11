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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.PipeliteTestConstants;
import pipelite.UniqueStringGenerator;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.exception.PipeliteRetryException;
import pipelite.process.ProcessState;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=ScheduleServiceTest"})
@DirtiesContext
@ActiveProfiles("test")
@Transactional
class ScheduleServiceTest {

  @Autowired ScheduleService scheduleService;

  @Test
  public void lifecycle() {
    String serviceName = UniqueStringGenerator.randomServiceName(ScheduleServiceTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(ScheduleServiceTest.class);
    String cron = PipeliteTestConstants.CRON_EVERY_TWO_SECONDS;
    String description = CronUtils.describe(cron);

    ScheduleEntity s = scheduleService.createSchedule(serviceName, pipelineName, cron);
    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(description);
    assertThat(s.getExecutionCount()).isZero();
    assertThat(s.getProcessId()).isNull();
    assertThat(s.getStartTime()).isNull();
    assertThat(s.getEndTime()).isNull();
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getLastCompleted()).isNull();
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(0);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.isFailed()).isEqualTo(false);
    assertThat(s.isActive()).isEqualTo(false);

    // Get all schedules.

    List<ScheduleEntity> schedules = scheduleService.getSchedules(serviceName);
    assertThat(schedules.size()).isEqualTo(1);
    assertThat(schedules.get(0)).isEqualTo(s);

    // Get active schedules.

    List<ScheduleEntity> activeSchedules = scheduleService.getSchedules(serviceName);
    assertThat(activeSchedules.size()).isEqualTo(1);
    assertThat(activeSchedules.get(0)).isEqualTo(s);

    // Schedule first execution.

    ZonedDateTime nextTime1 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    s = scheduleService.scheduleExecution(s, nextTime1);

    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(description);
    assertThat(s.getExecutionCount()).isEqualTo(0);
    assertThat(s.getNextTime()).isEqualTo(nextTime1);
    assertThat(s.getStartTime()).isNull();
    assertThat(s.getEndTime()).isNull();
    assertThat(s.getLastCompleted()).isNull();
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(0);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isNull();
    assertThat(s.isFailed()).isEqualTo(false);
    assertThat(s.isActive()).isEqualTo(false);

    // Start first execution.

    String processId1 = "test1";

    s = scheduleService.startExecution(pipelineName, processId1);

    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(description);
    assertThat(s.getExecutionCount()).isEqualTo(0);
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getStartTime()).isAfterOrEqualTo(nextTime1);
    assertThat(s.getEndTime()).isNull();
    assertThat(s.getLastCompleted()).isNull();
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(0);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId1);
    assertThat(s.isFailed()).isEqualTo(false);
    assertThat(s.isActive()).isEqualTo(true);

    // End first execution.

    ZonedDateTime endTime1 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);

    ProcessEntity processEntity1 = new ProcessEntity();
    processEntity1.setPipelineName(pipelineName);
    processEntity1.setProcessId(processId1);
    processEntity1.setProcessState(ProcessState.COMPLETED);

    ZonedDateTime nextTime2 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    s = scheduleService.endExecution(processEntity1, nextTime2);

    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(description);
    assertThat(s.getExecutionCount()).isEqualTo(1);
    assertThat(s.getNextTime()).isEqualTo(nextTime2);
    assertThat(s.getStartTime()).isAfterOrEqualTo(nextTime1);
    assertThat(s.getEndTime()).isAfterOrEqualTo(endTime1);
    assertThat(s.getLastCompleted()).isEqualTo(endTime1);
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(1);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId1);
    assertThat(s.isFailed()).isEqualTo(false);
    assertThat(s.isActive()).isEqualTo(false);

    // Start second execution.

    String processId2 = "test2";

    s = scheduleService.startExecution(pipelineName, processId2);

    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(description);
    assertThat(s.getExecutionCount()).isEqualTo(1);
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getStartTime()).isAfterOrEqualTo(nextTime2);
    assertThat(s.getEndTime()).isNull();
    assertThat(s.getLastCompleted()).isEqualTo(endTime1);
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(1);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId2);
    assertThat(s.isFailed()).isEqualTo(false);
    assertThat(s.isActive()).isEqualTo(true);

    // End second execution.

    ZonedDateTime endTime2 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);

    ProcessEntity processEntity2 = new ProcessEntity();
    processEntity2.setPipelineName(pipelineName);
    processEntity2.setProcessId(processId2);
    processEntity2.setProcessState(ProcessState.COMPLETED);

    ZonedDateTime nextTime3 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    s = scheduleService.endExecution(processEntity2, nextTime3);

    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(description);
    assertThat(s.getExecutionCount()).isEqualTo(2);
    assertThat(s.getNextTime()).isEqualTo(nextTime3);
    assertThat(s.getStartTime()).isAfterOrEqualTo(nextTime2);
    assertThat(s.getEndTime()).isAfterOrEqualTo(endTime2);
    assertThat(s.getLastCompleted()).isEqualTo(s.getEndTime());
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(2);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId2);
    assertThat(s.isFailed()).isEqualTo(false);
    assertThat(s.isActive()).isEqualTo(false);

    // Start third execution.

    String processId3 = "test3";

    s = scheduleService.startExecution(pipelineName, processId3);

    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(description);
    assertThat(s.getExecutionCount()).isEqualTo(2);
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getStartTime()).isAfterOrEqualTo(nextTime3);
    assertThat(s.getEndTime()).isNull();
    assertThat(s.getLastCompleted()).isAfterOrEqualTo(endTime2);
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(2);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId3);
    assertThat(s.isFailed()).isEqualTo(false);
    assertThat(s.isActive()).isEqualTo(true);

    // End third execution (FAILED).

    ZonedDateTime endTime3 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);

    ProcessEntity processEntity3 = new ProcessEntity();
    processEntity3.setPipelineName(pipelineName);
    processEntity3.setProcessId(processId3);
    processEntity3.setProcessState(ProcessState.FAILED);

    ZonedDateTime nextTime4 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    s = scheduleService.endExecution(processEntity3, nextTime4);

    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(description);
    assertThat(s.getExecutionCount()).isEqualTo(3);
    assertThat(s.getNextTime()).isEqualTo(nextTime4);
    assertThat(s.getStartTime()).isAfterOrEqualTo(nextTime3);
    assertThat(s.getEndTime()).isAfterOrEqualTo(endTime3);
    assertThat(s.getLastCompleted()).isAfterOrEqualTo(endTime2);
    assertThat(s.getLastFailed()).isEqualTo(s.getEndTime());
    assertThat(s.getStreakCompleted()).isEqualTo(0);
    assertThat(s.getStreakFailed()).isEqualTo(1);
    assertThat(s.getProcessId()).isEqualTo(processId3);
    assertThat(s.isFailed()).isEqualTo(true);
    assertThat(s.isActive()).isEqualTo(false);
  }

  @Test
  public void isRetryScheduleWithFailedSchedule() {
    String serviceName = UniqueStringGenerator.randomServiceName(ScheduleServiceTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());

    String cron = PipeliteTestConstants.CRON_EVERY_TWO_SECONDS;
    ScheduleEntity scheduleEntity = scheduleService.createSchedule(serviceName, pipelineName, cron);
    assertThat(scheduleEntity.isFailed()).isFalse();

    scheduleEntity = scheduleService.startExecution(pipelineName, processId);
    assertThat(scheduleEntity.isFailed()).isFalse();

    ProcessEntity processEntity = ProcessEntity.createExecution(pipelineName, processId, 1);
    processEntity.endExecution(ProcessState.FAILED);
    scheduleEntity = scheduleService.endExecution(processEntity, ZonedDateTime.now().plusDays(1));
    assertThat(scheduleEntity.isFailed()).isTrue();

    assertThat(scheduleService.isRetrySchedule(pipelineName, processId)).isTrue();
  }

  @Test
  public void isRetryScheduleWithNotFailedSchedule() {
    String serviceName = UniqueStringGenerator.randomServiceName(ScheduleServiceTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());

    String cron = PipeliteTestConstants.CRON_EVERY_TWO_SECONDS;
    ScheduleEntity scheduleEntity = scheduleService.createSchedule(serviceName, pipelineName, cron);
    assertThat(scheduleEntity.isFailed()).isFalse();

    PipeliteRetryException ex =
        assertThrows(
            PipeliteRetryException.class,
            () -> scheduleService.isRetrySchedule(pipelineName, processId));
    assertThat(ex.getMessage()).contains("schedule is not failed");
  }

  @Test
  public void isRetryScheduleWithDifferentProcessId() {
    String serviceName = UniqueStringGenerator.randomServiceName(ScheduleServiceTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    String differentProcessId = UniqueStringGenerator.randomProcessId(this.getClass());

    String cron = PipeliteTestConstants.CRON_EVERY_TWO_SECONDS;
    ScheduleEntity scheduleEntity = scheduleService.createSchedule(serviceName, pipelineName, cron);
    assertThat(scheduleEntity.isFailed()).isFalse();

    scheduleEntity = scheduleService.startExecution(pipelineName, processId);
    assertThat(scheduleEntity.isFailed()).isFalse();

    ProcessEntity processEntity = ProcessEntity.createExecution(pipelineName, processId, 1);
    processEntity.endExecution(ProcessState.FAILED);
    scheduleEntity = scheduleService.endExecution(processEntity, ZonedDateTime.now());
    assertThat(scheduleEntity.isFailed()).isTrue();

    PipeliteRetryException ex =
        assertThrows(
            PipeliteRetryException.class,
            () -> scheduleService.isRetrySchedule(pipelineName, differentProcessId));
    assertThat(ex.getMessage()).contains("last execution is a different process");
  }

  @Test
  public void isRetryScheduleWithNewExecutionWithinRetryMargin() {
    String serviceName = UniqueStringGenerator.randomServiceName(ScheduleServiceTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());

    String cron = PipeliteTestConstants.CRON_EVERY_TWO_SECONDS;
    ScheduleEntity scheduleEntity = scheduleService.createSchedule(serviceName, pipelineName, cron);
    assertThat(scheduleEntity.isFailed()).isFalse();

    scheduleEntity = scheduleService.startExecution(pipelineName, processId);
    assertThat(scheduleEntity.isFailed()).isFalse();

    ProcessEntity processEntity = ProcessEntity.createExecution(pipelineName, processId, 1);
    processEntity.endExecution(ProcessState.FAILED);
    scheduleEntity = scheduleService.endExecution(processEntity, ZonedDateTime.now());
    assertThat(scheduleEntity.isFailed()).isTrue();

    PipeliteRetryException ex =
        assertThrows(
            PipeliteRetryException.class,
            () -> scheduleService.isRetrySchedule(pipelineName, processId));
    assertThat(ex.getMessage()).contains("next execution is in less than");
  }

  @Test
  public void isRetryScheduleWithMissingSchedule() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());

    assertThat(scheduleService.getSavedSchedule(pipelineName).isPresent()).isFalse();
    assertThat(scheduleService.isRetrySchedule(pipelineName, processId)).isFalse();
  }
}
