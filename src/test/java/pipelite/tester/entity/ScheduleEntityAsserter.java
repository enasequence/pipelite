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
package pipelite.tester.entity;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import java.util.Collection;
import pipelite.entity.ScheduleEntity;
import pipelite.service.ScheduleService;
import pipelite.tester.TestType;
import pipelite.tester.TestTypeConfiguration;

public class ScheduleEntityAsserter {

  public static void assertCompletedScheduleEntity(
      ScheduleService scheduleService,
      TestTypeConfiguration testConfiguration,
      String serviceName,
      String pipelineName,
      int processCnt,
      Collection<String> processIds) {

    ScheduleEntity scheduleEntity = scheduleService.getSavedSchedule(pipelineName).get();

    TestType testType = testConfiguration.testType();
    assertThat(scheduleEntity.getServiceName()).isEqualTo(serviceName);
    assertThat(scheduleEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleEntity.getProcessId()).isEqualTo(Iterables.getLast(processIds));
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(processCnt);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isAfter(scheduleEntity.getStartTime());
    if (testType == TestType.NON_PERMANENT_ERROR || testType == TestType.PERMANENT_ERROR) {
      assertThat(scheduleEntity.getLastFailed()).isAfter(scheduleEntity.getStartTime());
      assertThat(scheduleEntity.getLastCompleted()).isNull();
      assertThat(scheduleEntity.getStreakFailed()).isEqualTo(processCnt);
      assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(0);
    } else {
      assertThat(scheduleEntity.getLastFailed()).isNull();
      assertThat(scheduleEntity.getLastCompleted()).isAfter(scheduleEntity.getStartTime());
      assertThat(scheduleEntity.getStreakFailed()).isEqualTo(0);
      assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(processCnt);
    }
  }
}
