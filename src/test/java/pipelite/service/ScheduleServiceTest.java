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

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.process.ProcessState;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@Transactional
class ScheduleServiceTest {

  @Autowired ScheduleService service;

  @Test
  public void lifecycle() {
    String serviceName = UniqueStringGenerator.randomServiceName(ScheduleServiceTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(ScheduleServiceTest.class);
    String cron = "0/1 * * * * ?"; // every second
    String description = CronUtils.describe(cron);

    ScheduleEntity scheduleEntity = new ScheduleEntity();
    scheduleEntity.setServiceName(serviceName);
    scheduleEntity.setPipelineName(pipelineName);
    scheduleEntity.setCron(cron);
    scheduleEntity.setDescription(description);

    service.saveSchedule(scheduleEntity);

    // Get all schedules.

    List<ScheduleEntity> schedules = service.getSchedules(serviceName);
    assertThat(schedules.size()).isEqualTo(1);
    assertThat(schedules.get(0)).isEqualTo(scheduleEntity);

    // Get active schedules.

    List<ScheduleEntity> activeSchedules = service.getSchedules(serviceName);
    assertThat(activeSchedules.size()).isEqualTo(1);
    assertThat(activeSchedules.get(0)).isEqualTo(scheduleEntity);

    // Schedule first execution.
    ScheduleEntity s = service.getSavedSchedule(pipelineName).get();

    ZonedDateTime nextTime1 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    service.scheduleExecution(s, nextTime1);

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

    // Start first execution.

    String processId1 = "test1";

    service.startExecution(pipelineName, processId1);

    s = service.getSavedSchedule(pipelineName).get();
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

    // End first execution.

    ZonedDateTime endTime1 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);

    ProcessEntity processEntity1 = new ProcessEntity();
    processEntity1.setPipelineName(pipelineName);
    processEntity1.setProcessId(processId1);
    processEntity1.setProcessState(ProcessState.COMPLETED);

    ZonedDateTime nextTime2 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    service.endExecution(processEntity1, nextTime2);

    s = service.getSavedSchedule(pipelineName).get();
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

    // Start second execution.

    String processId2 = "test2";

    service.startExecution(pipelineName, processId2);

    s = service.getSavedSchedule(pipelineName).get();
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

    // End second execution.

    ZonedDateTime endTime2 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);

    ProcessEntity processEntity2 = new ProcessEntity();
    processEntity2.setPipelineName(pipelineName);
    processEntity2.setProcessId(processId2);
    processEntity2.setProcessState(ProcessState.COMPLETED);

    ZonedDateTime nextTime3 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    service.endExecution(processEntity2, nextTime3);

    s = service.getSavedSchedule(pipelineName).get();
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

    // Start third execution.

    String processId3 = "test3";

    service.startExecution(pipelineName, processId3);

    s = service.getSavedSchedule(pipelineName).get();
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

    // End third execution (FAILED).

    ZonedDateTime endTime3 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);

    ProcessEntity processEntity3 = new ProcessEntity();
    processEntity3.setPipelineName(pipelineName);
    processEntity3.setProcessId(processId3);
    processEntity3.setProcessState(ProcessState.FAILED);

    ZonedDateTime nextTime4 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    service.endExecution(processEntity3, nextTime4);

    s = service.getSavedSchedule(pipelineName).get();
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
  }
}
