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
package pipelite.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import pipelite.TestProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessFactoryService;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;
import pipelite.stage.StageExecutionResultType;
import pipelite.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class PipeliteSchedulerTest {

  @Test
  public void nextProcessId() {
    assertThat(PipeliteScheduler.nextProcessId(null)).isEqualTo("1");
    assertThat(PipeliteScheduler.nextProcessId("0")).isEqualTo("1");
    assertThat(PipeliteScheduler.nextProcessId("1")).isEqualTo("2");
    assertThat(PipeliteScheduler.nextProcessId("9")).isEqualTo("10");
    assertThat(PipeliteScheduler.nextProcessId("10")).isEqualTo("11");
    assertThat(PipeliteScheduler.nextProcessId("29")).isEqualTo("30");
    assertThat(PipeliteScheduler.nextProcessId("134232")).isEqualTo("134233");
  }

  private static Process testProcess(String processId) {
    return new ProcessBuilder(processId)
        .execute("STAGE")
        .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
        .build();
  }

  @Test
  public void lifecycle() {
    String launcherName = UniqueStringGenerator.randomLauncherName();
    String pipelineName1 = UniqueStringGenerator.randomPipelineName();
    String pipelineName2 = UniqueStringGenerator.randomPipelineName();
    int maxExecution1 = 5;
    int maxExecution2 = 5;

    // Create launcher configuration with schedule refresh frequency.

    Duration scheduleRefreshFrequency = Duration.ofSeconds(5);
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setSchedulerName(launcherName);
    launcherConfiguration.setScheduleRefreshFrequency(scheduleRefreshFrequency);

    // Create two schedules.

    ScheduleEntity scheduleEntity1 = new ScheduleEntity();
    ScheduleEntity scheduleEntity2 = new ScheduleEntity();
    scheduleEntity1.setPipelineName(pipelineName1);
    scheduleEntity2.setPipelineName(pipelineName2);

    scheduleEntity1.setCron("0/2 * * * * ?"); // every two seconds
    scheduleEntity2.setCron("0/1 * * * * ?"); // every second

    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);
    ProcessFactoryService processFactoryService = mock(ProcessFactoryService.class);
    ScheduleService scheduleService = mock(ScheduleService.class);

    ProcessFactory processFactory1 =
        new TestProcessFactory(pipelineName1, Arrays.asList(testProcess("1")));
    ProcessFactory processFactory2 =
        new TestProcessFactory(pipelineName2, Arrays.asList(testProcess("1")));
    doAnswer(I -> processFactory1).when(processFactoryService).create(eq(pipelineName1));
    doAnswer(I -> processFactory2).when(processFactoryService).create(eq(pipelineName2));

    // Return both schedules from the schedule service.

    doReturn(Arrays.asList(scheduleEntity1, scheduleEntity2))
        .when(scheduleService)
        .getAllProcessSchedules(any());

    // Create process service to create process entities.

    ProcessService processService = mock(ProcessService.class);
    doAnswer(
            I -> {
              ProcessEntity processEntity = new ProcessEntity();
              processEntity.setPipelineName(I.getArgument(0));
              processEntity.setProcessId(I.getArgument(1));
              return processEntity;
            })
        .when(processService)
        .createExecution(any(), any(), any());

    // Create process launcher pool.

    ProcessRunnerPool processRunnerPool = mock(ProcessRunnerPool.class);
    doAnswer(
            I -> {
              Process process = I.getArgument(1);
              ProcessRunnerCallback callback = I.getArgument(2);
              callback.accept(process, mock(ProcessRunnerResult.class));
              return null;
            })
        .when(processRunnerPool)
        .runProcess(any(), any(), any());

    // Create pipelite scheduler.

    PipeliteScheduler pipeliteScheduler =
        new PipeliteScheduler(
            launcherConfiguration,
            pipeliteLocker,
            processFactoryService,
            scheduleService,
            processService,
            () -> processRunnerPool);
    pipeliteScheduler.setMaximumExecutions(pipelineName1, maxExecution1);
    pipeliteScheduler.setMaximumExecutions(pipelineName2, maxExecution2);

    // Check that there are no schedules yet and that new schedules can be created.

    assertThat(pipeliteScheduler.getExecutableSchedules().count()).isZero();
    assertThat(pipeliteScheduler.getPendingSchedules().count()).isZero();
    assertThat(pipeliteScheduler.isRefreshSchedules()).isTrue();

    // Create new schedules. The schedules are not immediately executable. The schedules are not
    // allowed to be immediately refreshed.

    pipeliteScheduler.startUp();

    assertThat(pipeliteScheduler.getExecutableSchedules().count()).isZero();
    assertThat(pipeliteScheduler.getPendingSchedules().count()).isEqualTo(2);
    assertThat(pipeliteScheduler.isRefreshSchedules()).isFalse();

    // Wait for the two schedules to be allowed to be refreshed.

    Time.wait(scheduleRefreshFrequency.plusMillis(1));

    assertThat(pipeliteScheduler.getExecutableSchedules().count()).isEqualTo(2);
    assertThat(pipeliteScheduler.getPendingSchedules().count()).isEqualTo(2);
    assertThat(pipeliteScheduler.isRefreshSchedules()).isTrue();
    LocalDateTime launchTime1 =
        pipeliteScheduler.getExecutableSchedules().findFirst().get().getLaunchTime();
    LocalDateTime launchTime2 =
        pipeliteScheduler.getExecutableSchedules().skip(1).findFirst().get().getLaunchTime();

    // Check that no processes have been executed yet.

    assertThat(
            pipeliteScheduler
                .getStats(pipelineName1)
                .getProcessExecutionCount(ProcessState.COMPLETED))
        .isEqualTo(0);
    assertThat(
            pipeliteScheduler
                .getStats(pipelineName2)
                .getProcessExecutionCount(ProcessState.COMPLETED))
        .isEqualTo(0);

    // Refresh the schedules and check that the launch times do not change. The schedules are not
    // allowed to be immediately refreshed.

    pipeliteScheduler.refreshSchedules();

    assertThat(pipeliteScheduler.getExecutableSchedules().count()).isEqualTo(2);
    assertThat(pipeliteScheduler.getPendingSchedules().count()).isEqualTo(2);
    assertThat(pipeliteScheduler.isRefreshSchedules()).isFalse();
    assertThat(launchTime1)
        .isEqualTo(pipeliteScheduler.getExecutableSchedules().findFirst().get().getLaunchTime());
    assertThat(launchTime2)
        .isEqualTo(
            pipeliteScheduler.getExecutableSchedules().skip(1).findFirst().get().getLaunchTime());

    // Run the schedules and check that the launch times have been updated. The schedules are not be
    // immediately executable and are not allowed to be immediately refreshed.

    pipeliteScheduler.run();

    while (pipeliteScheduler.getRunningSchedules().count() > 0) {
      Time.wait(100, TimeUnit.MILLISECONDS);
    }

    assertThat(pipeliteScheduler.getExecutableSchedules().count()).isZero();
    assertThat(pipeliteScheduler.getPendingSchedules().count()).isEqualTo(2);
    assertThat(pipeliteScheduler.isRefreshSchedules()).isFalse();
    assertThat(launchTime1)
        .isBefore(pipeliteScheduler.getPendingSchedules().findFirst().get().getLaunchTime());
    assertThat(launchTime2)
        .isBefore(
            pipeliteScheduler.getPendingSchedules().skip(1).findFirst().get().getLaunchTime());
  }
}
