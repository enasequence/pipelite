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

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.FullTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteSchedule;

import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test"})
class HsqlPipeliteScheduleServiceTest {

  @Autowired PipeliteScheduleService service;

  @Test
  @Transactional
  @Rollback
  public void testCrud() {

    String launcherName = UniqueStringGenerator.randomLauncherName();
    String processName = UniqueStringGenerator.randomProcessName();
    String processName2 = UniqueStringGenerator.randomProcessName();

    PipeliteSchedule schedule = new PipeliteSchedule();
    schedule.setLauncherName(launcherName);
    schedule.setProcessName(processName);
    schedule.setProcessFactoryName("test");
    schedule.setSchedule("test");

    service.saveProcessSchedule(schedule);

    List<PipeliteSchedule> schedules = service.getAllProcessSchedules(launcherName);
    assertThat(schedules.size()).isEqualTo(1);
    assertThat(schedules.get(0).getLauncherName()).isEqualTo(schedule.getLauncherName());
    assertThat(schedules.get(0).getProcessName()).isEqualTo(schedule.getProcessName());
    assertThat(schedules.get(0).getProcessFactoryName())
        .isEqualTo(schedule.getProcessFactoryName());
    assertThat(schedules.get(0).getSchedule()).isEqualTo(schedule.getSchedule());
    assertThat(schedules.get(0).getExecutionCount()).isEqualTo(0);
    assertThat(schedules.get(0).getStartTime()).isNull();
    assertThat(schedules.get(0).getEndTime()).isNull();

    PipeliteSchedule schedule2 = new PipeliteSchedule();
    schedule2.setLauncherName(launcherName);
    schedule2.setProcessName(processName2);
    schedule2.setProcessFactoryName("test2");
    schedule2.setSchedule("test2");

    service.saveProcessSchedule(schedule2);

    schedules = service.getAllProcessSchedules(launcherName);
    schedules.sort(Comparator.comparing(PipeliteSchedule::getSchedule));
    assertThat(schedules.size()).isEqualTo(2);
    assertThat(schedules.get(0).getLauncherName()).isEqualTo(schedule.getLauncherName());
    assertThat(schedules.get(0).getProcessName()).isEqualTo(schedule.getProcessName());
    assertThat(schedules.get(0).getProcessFactoryName())
        .isEqualTo(schedule.getProcessFactoryName());
    assertThat(schedules.get(0).getSchedule()).isEqualTo(schedule.getSchedule());
    assertThat(schedules.get(0).getExecutionCount()).isEqualTo(0);
    assertThat(schedules.get(0).getStartTime()).isNull();
    assertThat(schedules.get(0).getEndTime()).isNull();

    assertThat(schedules.get(1).getLauncherName()).isEqualTo(schedule2.getLauncherName());
    assertThat(schedules.get(1).getProcessName()).isEqualTo(schedule2.getProcessName());
    assertThat(schedules.get(1).getProcessFactoryName())
        .isEqualTo(schedule2.getProcessFactoryName());
    assertThat(schedules.get(1).getSchedule()).isEqualTo(schedule2.getSchedule());
    assertThat(schedules.get(1).getExecutionCount()).isEqualTo(0);
    assertThat(schedules.get(1).getStartTime()).isNull();
    assertThat(schedules.get(1).getEndTime()).isNull();
  }
}
