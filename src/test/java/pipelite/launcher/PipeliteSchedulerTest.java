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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pipelite.TestProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.launcher.process.runner.ProcessRunnerCallback;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerResult;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessFactoryService;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;
import pipelite.stage.executor.StageExecutorResultType;
import pipelite.time.Time;

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
        .withEmptySyncExecutor(StageExecutorResultType.SUCCESS)
        .build();
  }

  @Test
  @Timeout(10)
  public void refresh() {
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
            () -> processRunnerPool,
            new SimpleMeterRegistry());
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
    ZonedDateTime launchTime1 =
        pipeliteScheduler.getExecutableSchedules().findFirst().get().getLaunchTime();
    ZonedDateTime launchTime2 =
        pipeliteScheduler.getExecutableSchedules().skip(1).findFirst().get().getLaunchTime();

    // Check that no processes have been executed yet.

    verify(processRunnerPool, times(0)).runProcess(any(), any(), any());

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

    // Run the scheduler and check that the launch times have been removed. The schedules are not be
    // immediately executable or pending.

    pipeliteScheduler.run();

    while (pipeliteScheduler.getRunningSchedules().count() > 0) {
      Time.wait(100, TimeUnit.MILLISECONDS);
    }

    verify(processRunnerPool, times(2)).runProcess(any(), any(), any());
    assertThat(pipeliteScheduler.getExecutableSchedules().count()).isZero();
    assertThat(pipeliteScheduler.getPendingSchedules().count()).isZero();
    assertThat(pipeliteScheduler.getActiveSchedules().findFirst().get().getLaunchTime()).isNull();
    assertThat(pipeliteScheduler.getActiveSchedules().skip(1).findFirst().get().getLaunchTime())
        .isNull();

    // Run the scheduler and check that the launch times have been set.

    pipeliteScheduler.run();

    verify(processRunnerPool, times(2)).runProcess(any(), any(), any());
    assertThat(pipeliteScheduler.getPendingSchedules().count()).isEqualTo(2);
    assertThat(launchTime1)
        .isBefore(pipeliteScheduler.getPendingSchedules().findFirst().get().getLaunchTime());
    assertThat(launchTime2)
        .isBefore(
            pipeliteScheduler.getPendingSchedules().skip(1).findFirst().get().getLaunchTime());
  }

  @Test
  public void resume() {
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

    // Create two schedules with start time and process id to allow processes to resume.

    ZonedDateTime launchTime1 = ZonedDateTime.now().minusHours(1);
    ZonedDateTime launchTime2 = ZonedDateTime.now().minusHours(1);

    ScheduleEntity scheduleEntity1 = new ScheduleEntity();
    ScheduleEntity scheduleEntity2 = new ScheduleEntity();
    scheduleEntity1.setPipelineName(pipelineName1);
    scheduleEntity2.setPipelineName(pipelineName2);
    scheduleEntity1.setStartTime(launchTime1);
    scheduleEntity2.setStartTime(launchTime2);
    String processId1 = "1";
    String processId2 = "2";
    scheduleEntity1.setProcessId(processId1);
    scheduleEntity2.setProcessId(processId2);

    scheduleEntity1.setCron("0/2 * * * * ?"); // every two seconds
    scheduleEntity2.setCron("0/1 * * * * ?"); // every second

    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);
    ProcessFactoryService processFactoryService = mock(ProcessFactoryService.class);
    ScheduleService scheduleService = mock(ScheduleService.class);

    ProcessFactory processFactory1 =
        new TestProcessFactory(pipelineName1, Arrays.asList(testProcess(processId1)));
    ProcessFactory processFactory2 =
        new TestProcessFactory(pipelineName2, Arrays.asList(testProcess(processId2)));
    doAnswer(I -> processFactory1).when(processFactoryService).create(eq(pipelineName1));
    doAnswer(I -> processFactory2).when(processFactoryService).create(eq(pipelineName2));

    // Return both schedules from the schedule service.

    doReturn(Arrays.asList(scheduleEntity1, scheduleEntity2))
        .when(scheduleService)
        .getAllProcessSchedules(any());

    // Create process service to return saved process entities.

    ProcessService processService = mock(ProcessService.class);
    doAnswer(
            I -> {
              ProcessEntity processEntity = new ProcessEntity();
              processEntity.setPipelineName(I.getArgument(0));
              processEntity.setProcessId(I.getArgument(1));
              return Optional.of(processEntity);
            })
        .when(processService)
        .getSavedProcess(any(), any());

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
        spy(
            new PipeliteScheduler(
                launcherConfiguration,
                pipeliteLocker,
                processFactoryService,
                scheduleService,
                processService,
                () -> processRunnerPool,
                new SimpleMeterRegistry()));
    pipeliteScheduler.setMaximumExecutions(pipelineName1, maxExecution1);
    pipeliteScheduler.setMaximumExecutions(pipelineName2, maxExecution2);

    // Resume the two processes, check that they are immediately executed
    // and that they are scheduled for a later execution.

    pipeliteScheduler.startUp();
    verify(processRunnerPool, times(1)).runProcess(eq(pipelineName1), any(), any());
    verify(processRunnerPool, times(1)).runProcess(eq(pipelineName2), any(), any());

    assertThat(pipeliteScheduler.getExecutableSchedules().count()).isEqualTo(0);
    assertThat(pipeliteScheduler.getPendingSchedules().count()).isEqualTo(2);
    assertThat(pipeliteScheduler.isRefreshSchedules()).isFalse();
    assertThat(launchTime1)
        .isBefore(pipeliteScheduler.getPendingSchedules().findFirst().get().getLaunchTime());
    assertThat(launchTime2)
        .isBefore(
            pipeliteScheduler.getPendingSchedules().skip(1).findFirst().get().getLaunchTime());
  }

  @Test
  public void invalidCron() {
    String launcherName = UniqueStringGenerator.randomLauncherName();
    String pipelineName1 = UniqueStringGenerator.randomPipelineName();

    // Create launcher configuration with schedule refresh frequency.

    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setSchedulerName(launcherName);

    // Create schedule that has invalid cron.

    ScheduleEntity scheduleEntity1 = new ScheduleEntity();
    scheduleEntity1.setPipelineName(pipelineName1);
    scheduleEntity1.setCron("invalid");

    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);
    ProcessFactoryService processFactoryService = mock(ProcessFactoryService.class);
    ScheduleService scheduleService = mock(ScheduleService.class);
    ProcessService processService = mock(ProcessService.class);
    ProcessRunnerPool processRunnerPool = mock(ProcessRunnerPool.class);

    // Return schedule from the schedule service.

    doReturn(Arrays.asList(scheduleEntity1)).when(scheduleService).getAllProcessSchedules(any());

    // Create pipelite scheduler.

    PipeliteScheduler pipeliteScheduler =
        spy(
            new PipeliteScheduler(
                launcherConfiguration,
                pipeliteLocker,
                processFactoryService,
                scheduleService,
                processService,
                () -> processRunnerPool,
                new SimpleMeterRegistry()));
    int maxExecution1 = 1;
    pipeliteScheduler.setMaximumExecutions(pipelineName1, maxExecution1);

    pipeliteScheduler.startUp();

    verify(processRunnerPool, times(0)).runProcess(any(), any(), any());
    assertThat(pipeliteScheduler.getSchedules().count()).isZero();
  }

  @Test
  public void resumeNoProcess() {
    String launcherName = UniqueStringGenerator.randomLauncherName();
    String pipelineName1 = UniqueStringGenerator.randomPipelineName();

    // Create launcher configuration with schedule refresh frequency.

    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setSchedulerName(launcherName);

    // Create schedule with start time and process id to allow processes to resume.

    ZonedDateTime launchTime1 = ZonedDateTime.now().minusHours(1);

    ScheduleEntity scheduleEntity1 = new ScheduleEntity();
    scheduleEntity1.setPipelineName(pipelineName1);
    scheduleEntity1.setStartTime(launchTime1);
    String processId1 = "1";
    scheduleEntity1.setProcessId(processId1);
    scheduleEntity1.setCron("0/2 * * * * ?"); // every two seconds

    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);
    ProcessFactoryService processFactoryService = mock(ProcessFactoryService.class);
    ScheduleService scheduleService = mock(ScheduleService.class);
    ProcessService processService = mock(ProcessService.class);
    ProcessRunnerPool processRunnerPool = mock(ProcessRunnerPool.class);

    // Create process factory.

    ProcessFactory processFactory1 =
        new TestProcessFactory(pipelineName1, Arrays.asList(testProcess(processId1)));
    doAnswer(I -> processFactory1).when(processFactoryService).create(eq(pipelineName1));

    // Return schedule from the schedule service.

    doReturn(Arrays.asList(scheduleEntity1)).when(scheduleService).getAllProcessSchedules(any());

    // Create pipelite scheduler.

    PipeliteScheduler pipeliteScheduler =
        spy(
            new PipeliteScheduler(
                launcherConfiguration,
                pipeliteLocker,
                processFactoryService,
                scheduleService,
                processService,
                () -> processRunnerPool,
                new SimpleMeterRegistry()));
    int maxExecution1 = 1;
    pipeliteScheduler.setMaximumExecutions(pipelineName1, maxExecution1);

    pipeliteScheduler.startUp();

    verify(pipeliteScheduler, times(1)).resumeProcesses();
    verify(pipeliteScheduler, times(1)).resumeProcess(any());
    verify(processRunnerPool, times(0)).runProcess(any(), any(), any());
    verify(pipeliteScheduler, times(1)).scheduleProcesses();
    verify(pipeliteScheduler, times(1)).scheduleProcess(any());
    assertThat(pipeliteScheduler.getExecutableSchedules().count()).isEqualTo(0);
    assertThat(pipeliteScheduler.getPendingSchedules().count()).isEqualTo(1);
  }
}
