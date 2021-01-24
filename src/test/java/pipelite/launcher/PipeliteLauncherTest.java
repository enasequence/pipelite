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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Value;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import pipelite.Pipeline;
import pipelite.PipeliteTestBeans;
import pipelite.PipeliteTestConfiguration;
import pipelite.TestProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.WebConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.entity.StageLogEntity;
import pipelite.launcher.process.creator.ProcessCreator;
import pipelite.launcher.process.queue.DefaultProcessQueue;
import pipelite.launcher.process.runner.DefaultProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.lock.PipeliteLocker;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessSource;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.*;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.launcher.processRunnerFrequency=250ms",
      "pipelite.launcher.shutdownIfIdle=true"
    })
@DirtiesContext
public class PipeliteLauncherTest {

  @Autowired private WebConfiguration webConfiguration;
  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ExecutorConfiguration executorConfiguration;
  @Autowired private RegisteredPipelineService registeredPipelineService;
  @Autowired private ProcessSourceService processSourceService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private LockService lockService;
  @Autowired private MailService mailService;
  @Autowired private PipeliteMetrics pipelineMetrics;
  @Autowired private PipeliteMetrics metrics;

  @Autowired
  @Qualifier("processSuccess")
  private TestPipeline processSuccess;

  @Autowired
  @Qualifier("processFailure")
  private TestPipeline processFailure;

  @Autowired
  @Qualifier("processException")
  private TestPipeline processException;

  @TestConfiguration
  static class TestConfig {
    @Bean("processSuccess")
    @Primary
    public TestPipeline processSuccess() {
      return new TestPipeline("processSuccess", 4, 2, StageTestResult.SUCCESS);
    }

    @Bean("processFailure")
    public TestPipeline processFailure() {
      return new TestPipeline("processFailure", 4, 2, StageTestResult.ERROR);
    }

    @Bean("processException")
    public TestPipeline processException() {
      return new TestPipeline("processException", 4, 2, StageTestResult.EXCEPTION);
    }

    @Bean
    public ProcessSource processSuccessSource(
        @Autowired @Qualifier("processSuccess") TestPipeline f) {
      return new TestProcessSource(f.getPipelineName(), f.processCnt);
    }

    @Bean
    public ProcessSource processFailureSource(
        @Autowired @Qualifier("processFailure") TestPipeline f) {
      return new TestProcessSource(f.getPipelineName(), f.processCnt);
    }

    @Bean
    public ProcessSource processExceptionSource(
        @Autowired @Qualifier("processException") TestPipeline f) {
      return new TestProcessSource(f.getPipelineName(), f.processCnt);
    }
  }

  private enum StageTestResult {
    SUCCESS,
    ERROR,
    EXCEPTION
  }

  private PipeliteLauncher createPipeliteLauncher(String pipelineName) {
    return DefaultPipeliteLauncher.create(
        webConfiguration,
        launcherConfiguration,
        executorConfiguration,
        lockService,
        registeredPipelineService,
        processSourceService,
        processService,
        stageService,
        mailService,
        pipelineMetrics,
        pipelineName);
  }

  @Value
  public static class TestPipeline implements Pipeline {
    private final String pipelineName;
    public final int processCnt;
    public final int stageCnt;
    public final StageTestResult stageTestResult;
    public final List<String> processIds = Collections.synchronizedList(new ArrayList<>());
    public final AtomicLong stageExecCnt = new AtomicLong();

    public TestPipeline(
        String pipelineNamePrefix, int processCnt, int stageCnt, StageTestResult stageTestResult) {
      this.pipelineName = pipelineNamePrefix + "_" + UniqueStringGenerator.randomPipelineName();
      this.processCnt = processCnt;
      this.stageCnt = stageCnt;
      this.stageTestResult = stageTestResult;
    }

    public void reset() {
      processIds.clear();
      stageExecCnt.set(0L);
    }

    @Override
    public String getPipelineName() {
      return pipelineName;
    }

    @Override
    public int getPipelineParallelism() {
      return 5;
    }

    @Override
    public Process createProcess(ProcessBuilder builder) {
      processIds.add(builder.getProcessId());
      ExecutorParameters executorParams =
          ExecutorParameters.builder()
              .immediateRetries(0)
              .maximumRetries(0)
              .timeout(Duration.ofSeconds(10))
              .build();

      for (int i = 0; i < stageCnt; ++i) {
        builder
            .execute("STAGE" + i)
            .withCallExecutor(
                (request) -> {
                  stageExecCnt.incrementAndGet();
                  if (stageTestResult == StageTestResult.ERROR) {
                    return StageExecutorResult.error();
                  }
                  if (stageTestResult == StageTestResult.SUCCESS) {
                    return StageExecutorResult.success();
                  }
                  if (stageTestResult == StageTestResult.EXCEPTION) {
                    throw new RuntimeException("Expected exception");
                  }
                  throw new RuntimeException("Unexpected exception");
                },
                executorParams);
      }
      return builder.build();
    }
  }

  private void assertLauncherMetrics(TestPipeline f) {
    PipelineMetrics pipelineMetrics = metrics.pipeline(f.getPipelineName());

    if (f.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(pipelineMetrics.process().getFailedCount())
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(f.stageExecCnt.get());
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0L);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(f.stageExecCnt.get());
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(0);
    } else {
      assertThat(pipelineMetrics.process().getCompletedCount())
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(f.stageExecCnt.get());
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(f.stageExecCnt.get());
    }
  }

  private void assertProcessEntity(TestPipeline f, String processId) {
    String pipelineName = f.getPipelineName();

    ProcessEntity processEntity =
        processService.getSavedProcess(f.getPipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    if (f.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(processEntity.getProcessState())
          .isEqualTo(ProcessState.FAILED); // no re-executions allowed
    } else {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    }
  }

  private void assertStageEntities(TestPipeline f, String processId) {
    String pipelineName = f.getPipelineName();

    for (int i = 0; i < f.stageCnt; ++i) {
      StageEntity stageEntity =
          stageService.getSavedStage(f.getPipelineName(), processId, "STAGE" + i).get();
      StageLogEntity stageLogEntity =
          stageService.getSavedStageLog(f.getPipelineName(), processId, "STAGE" + i).get();
      assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
      assertThat(stageEntity.getProcessId()).isEqualTo(processId);
      assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
      assertThat(stageEntity.getStartTime()).isNotNull();
      assertThat(stageEntity.getEndTime()).isNotNull();
      assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
      assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.CallExecutor");
      assertThat(stageEntity.getExecutorData()).isNull();
      assertThat(stageEntity.getExecutorParams())
          .isEqualTo(
              "{\n"
                  + "  \"timeout\" : 10000,\n"
                  + "  \"maximumRetries\" : 0,\n"
                  + "  \"immediateRetries\" : 0\n"
                  + "}");

      if (f.stageTestResult == StageTestResult.ERROR) {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
        assertThat(stageEntity.getResultParams()).isNull();
      } else if (f.stageTestResult == StageTestResult.EXCEPTION) {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
        assertThat(stageLogEntity.getStageLog())
            .contains(
                "pipelite.exception.PipeliteException: java.lang.RuntimeException: Expected exception");
      } else {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
        assertThat(stageEntity.getResultParams()).isNull();
      }
    }
  }

  private void test(TestPipeline f) {
    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    assertThat(pipeliteLauncher.getActiveProcessRunners().size()).isEqualTo(0);

    assertThat(f.stageExecCnt.get() / f.stageCnt).isEqualTo(f.processCnt);
    assertThat(f.processIds.size()).isEqualTo(f.processCnt);
    assertLauncherMetrics(f);
    for (String processId : f.processIds) {
      assertProcessEntity(f, processId);
      assertStageEntities(f, processId);
    }
  }

  @Test
  public void testSuccess() {
    test(processSuccess);
  }

  @Test
  public void testFailure() {
    test(processFailure);
  }

  @Test
  public void testException() {
    test(processException);
  }

  @Test
  public void testRunProcess() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    WebConfiguration webConfiguration = new WebConfiguration();
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    int pipelineParallelism = ForkJoinPool.getCommonPoolParallelism();

    Pipeline pipeline =
        new Pipeline() {
          @Override
          public String getPipelineName() {
            return pipelineName;
          }

          @Override
          public int getPipelineParallelism() {
            return pipelineParallelism;
          }

          @Override
          public Process createProcess(ProcessBuilder builder) {
            return mock(Process.class);
          }
        };

    DefaultProcessQueue queue =
        spy(
            new DefaultProcessQueue(
                webConfiguration,
                launcherConfiguration,
                mock(ProcessService.class),
                pipelineName,
                pipeline.getPipelineParallelism()));

    List<ProcessEntity> processesEntities =
        Collections.nCopies(processCnt, mock(ProcessEntity.class));
    doReturn(processesEntities).when(queue).getAvailableActiveProcesses();

    ProcessRunnerPool pool = mock(ProcessRunnerPool.class);

    PipeliteMetrics metrics = PipeliteTestBeans.pipeliteMetrics();

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                mock(PipeliteLocker.class),
                pipeline,
                mock(ProcessCreator.class),
                queue,
                pool,
                metrics));

    launcher.startUp();
    launcher.run();

    verify(launcher, times(1)).run();
    verify(launcher, times(processCnt)).runProcess(any());
  }

  @Test
  public void testQueueProcesses() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    Duration refreshFrequency = Duration.ofDays(1);
    WebConfiguration webConfiguration = new WebConfiguration();
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setProcessQueueMaxRefreshFrequency(refreshFrequency);
    launcherConfiguration.setProcessQueueMinRefreshFrequency(refreshFrequency);
    int pipelineParallelism = ForkJoinPool.getCommonPoolParallelism();

    DefaultProcessQueue queue =
        spy(
            new DefaultProcessQueue(
                webConfiguration,
                launcherConfiguration,
                mock(ProcessService.class),
                pipelineName,
                pipelineParallelism));

    assertThat(queue.getProcessQueueMaxValidUntil()).isBeforeOrEqualTo(ZonedDateTime.now());
    assertThat(queue.getProcessQueueMinValidUntil()).isBeforeOrEqualTo(ZonedDateTime.now());

    List<ProcessEntity> processesEntities =
        Collections.nCopies(processCnt, mock(ProcessEntity.class));
    doReturn(processesEntities).when(queue).getAvailableActiveProcesses();
    doReturn(processesEntities).when(queue).getPendingProcesses();

    ProcessRunnerPool pool = mock(ProcessRunnerPool.class);
    PipeliteMetrics metrics = PipeliteTestBeans.pipeliteMetrics();

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                mock(PipeliteLocker.class),
                mock(Pipeline.class),
                mock(ProcessCreator.class),
                queue,
                pool,
                metrics));

    launcher.startUp();
    launcher.run();

    ZonedDateTime plusRefresh = ZonedDateTime.now().plus(refreshFrequency);
    ZonedDateTime plusBeforeRefresh = ZonedDateTime.now().plus(Duration.ofHours(23));
    assertThat(queue.getProcessQueueMaxValidUntil()).isAfter(plusBeforeRefresh);
    assertThat(queue.getProcessQueueMinValidUntil()).isAfter(plusBeforeRefresh);
    assertThat(plusRefresh.isAfter(queue.getProcessQueueMaxValidUntil()));
    assertThat(plusRefresh.isAfter(queue.getProcessQueueMinValidUntil()));

    verify(launcher, times(1)).run();
    verify(queue, times(1)).fillQueue();
    verify(queue, times(1)).getAvailableActiveProcesses();
    verify(queue, times(1)).getPendingProcesses();
  }

  @Test
  public void testCreateProcess() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    WebConfiguration webConfiguration = new WebConfiguration();
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setProcessCreateMaxSize(100);
    int pipelineParallelism = ForkJoinPool.getCommonPoolParallelism();

    ProcessCreator processCreator = mock(ProcessCreator.class);
    DefaultProcessQueue queue =
        spy(
            new DefaultProcessQueue(
                webConfiguration,
                launcherConfiguration,
                mock(ProcessService.class),
                pipelineName,
                pipelineParallelism));
    when(queue.isFillQueue()).thenReturn(true);

    PipeliteMetrics metrics = PipeliteTestBeans.pipeliteMetrics();

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                mock(PipeliteLocker.class),
                mock(Pipeline.class),
                processCreator,
                queue,
                mock(DefaultProcessRunnerPool.class),
                metrics));

    launcher.startUp();
    launcher.run();

    verify(launcher, times(1)).run();
    verify(processCreator, times(1)).createProcesses(processCnt);
  }

  @Test
  public void getLauncherName() {
    String hostName = WebConfiguration.getCanonicalHostName();
    assertThat(PipeliteLauncher.getLauncherName("TEST", 8080))
        .startsWith("TEST@" + hostName + ":8080:");
  }
}
