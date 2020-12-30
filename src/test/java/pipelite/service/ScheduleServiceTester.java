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
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.process.ProcessState;

class ScheduleServiceTester {

  public ScheduleServiceTester(ScheduleService service) {
    this.service = service;
  }

  private final ScheduleService service;

  public void lifecycle() {
    String schedulerName = UniqueStringGenerator.randomSchedulerName();
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String cron = "0/1 * * * * ?"; // every secondx§x§

    ScheduleEntity scheduleEntity = new ScheduleEntity();
    scheduleEntity.setSchedulerName(schedulerName);
    scheduleEntity.setPipelineName(pipelineName);
    scheduleEntity.setCron(cron);
    scheduleEntity.setActive(true);

    service.saveSchedule(scheduleEntity);

    // Get all schedules.

    List<ScheduleEntity> schedules = service.getSchedules(schedulerName);
    assertThat(schedules.size()).isEqualTo(1);
    assertThat(schedules.get(0)).isEqualTo(scheduleEntity);

    // Get active schedules.

    List<ScheduleEntity> activeSchedules = service.getActiveSchedules(schedulerName);
    assertThat(activeSchedules.size()).isEqualTo(1);
    assertThat(activeSchedules.get(0)).isEqualTo(scheduleEntity);

    // Schedule first execution.

    ZonedDateTime launchTime1 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    service.scheduleExecution(pipelineName, launchTime1);

    ScheduleEntity s = service.getSavedSchedule(pipelineName).get();
    assertThat(s.getSchedulerName()).isEqualTo(schedulerName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getActive()).isTrue();
    assertThat(s.getExecutionCount()).isEqualTo(0);
    assertThat(s.getNextTime()).isEqualTo(launchTime1);
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
    assertThat(s.getSchedulerName()).isEqualTo(schedulerName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getActive()).isTrue();
    assertThat(s.getExecutionCount()).isEqualTo(0);
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getStartTime()).isAfterOrEqualTo(launchTime1);
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
    processEntity1.setState(ProcessState.COMPLETED);

    service.endExecution(processEntity1);

    s = service.getSavedSchedule(pipelineName).get();
    assertThat(s.getSchedulerName()).isEqualTo(schedulerName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getActive()).isTrue();
    assertThat(s.getExecutionCount()).isEqualTo(1);
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getStartTime()).isAfterOrEqualTo(launchTime1);
    assertThat(s.getEndTime()).isAfterOrEqualTo(endTime1);
    assertThat(s.getLastCompleted()).isEqualTo(endTime1);
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(1);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId1);

    // Schedule second execution.

    ZonedDateTime launchTime2 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    service.scheduleExecution(pipelineName, launchTime2);

    s = service.getSavedSchedule(pipelineName).get();
    assertThat(s.getSchedulerName()).isEqualTo(schedulerName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getActive()).isTrue();
    assertThat(s.getExecutionCount()).isEqualTo(1);
    assertThat(s.getNextTime()).isEqualTo(launchTime2);
    assertThat(s.getStartTime()).isAfterOrEqualTo(launchTime1);
    assertThat(s.getEndTime()).isAfterOrEqualTo(endTime1);
    assertThat(s.getLastCompleted()).isAfterOrEqualTo(endTime1);
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(1);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId1);

    // Start second execution.

    String processId2 = "test2";

    service.startExecution(pipelineName, processId2);

    s = service.getSavedSchedule(pipelineName).get();
    assertThat(s.getSchedulerName()).isEqualTo(schedulerName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getActive()).isTrue();
    assertThat(s.getExecutionCount()).isEqualTo(1);
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getStartTime()).isAfterOrEqualTo(launchTime2);
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
    processEntity2.setState(ProcessState.COMPLETED);

    service.endExecution(processEntity2);

    s = service.getSavedSchedule(pipelineName).get();
    assertThat(s.getSchedulerName()).isEqualTo(schedulerName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getActive()).isTrue();
    assertThat(s.getExecutionCount()).isEqualTo(2);
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getStartTime()).isAfterOrEqualTo(launchTime2);
    assertThat(s.getEndTime()).isAfterOrEqualTo(endTime2);
    assertThat(s.getLastCompleted()).isEqualTo(s.getEndTime());
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(2);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId2);

    // Schedule third execution.

    ZonedDateTime launchTime3 = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    service.scheduleExecution(pipelineName, launchTime3);

    s = service.getSavedSchedule(pipelineName).get();
    assertThat(s.getSchedulerName()).isEqualTo(schedulerName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getActive()).isTrue();
    assertThat(s.getExecutionCount()).isEqualTo(2);
    assertThat(s.getNextTime()).isEqualTo(launchTime3);
    assertThat(s.getStartTime()).isAfterOrEqualTo(launchTime2);
    assertThat(s.getEndTime()).isAfterOrEqualTo(endTime2);
    assertThat(s.getLastCompleted()).isAfterOrEqualTo(endTime2);
    assertThat(s.getLastFailed()).isNull();
    assertThat(s.getStreakCompleted()).isEqualTo(2);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId2);

    // Start third execution.

    String processId3 = "test3";

    service.startExecution(pipelineName, processId3);

    s = service.getSavedSchedule(pipelineName).get();
    assertThat(s.getSchedulerName()).isEqualTo(schedulerName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getActive()).isTrue();
    assertThat(s.getExecutionCount()).isEqualTo(2);
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getStartTime()).isAfterOrEqualTo(launchTime3);
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
    processEntity3.setState(ProcessState.FAILED);

    service.endExecution(processEntity3);

    s = service.getSavedSchedule(pipelineName).get();
    assertThat(s.getSchedulerName()).isEqualTo(schedulerName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getActive()).isTrue();
    assertThat(s.getExecutionCount()).isEqualTo(3);
    assertThat(s.getNextTime()).isNull();
    assertThat(s.getStartTime()).isAfterOrEqualTo(launchTime3);
    assertThat(s.getEndTime()).isAfterOrEqualTo(endTime3);
    assertThat(s.getLastCompleted()).isAfterOrEqualTo(endTime2);
    assertThat(s.getLastFailed()).isEqualTo(s.getEndTime());
    assertThat(s.getStreakCompleted()).isEqualTo(0);
    assertThat(s.getStreakFailed()).isEqualTo(1);
    assertThat(s.getProcessId()).isEqualTo(processId3);
  }
}
