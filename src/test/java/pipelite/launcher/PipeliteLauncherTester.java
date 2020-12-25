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

import io.micrometer.core.instrument.MeterRegistry;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.TestProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.configuration.WebConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.launcher.process.runner.ProcessRunnerStats;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.*;
import pipelite.stage.executor.StageExecutorParameters;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultType;

@Component
@Scope("prototype")
public class PipeliteLauncherTester {

  @Autowired private WebConfiguration webConfiguration;
  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private StageConfiguration stageConfiguration;
  @Autowired private ProcessFactoryService processFactoryService;
  @Autowired private ProcessSourceService processSourceService;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private LockService lockService;
  @Autowired private MailService mailService;
  @Autowired private MeterRegistry meterRegistry;

  @Autowired
  @Qualifier("processSuccess")
  private TestProcessFactory processSuccess;

  @Autowired
  @Qualifier("processFailure")
  private TestProcessFactory processFailure;

  @Autowired
  @Qualifier("processException")
  private TestProcessFactory processException;

  @TestConfiguration
  static class TestConfig {
    @Bean("processSuccess")
    @Primary
    public TestProcessFactory processSuccess() {
      return new TestProcessFactory("processSuccess", 4, 2, StageTestResult.SUCCESS);
    }

    @Bean("processFailure")
    public TestProcessFactory processFailure() {
      return new TestProcessFactory("processFailure", 4, 2, StageTestResult.ERROR);
    }

    @Bean("processException")
    public TestProcessFactory processException() {
      return new TestProcessFactory("processException", 4, 2, StageTestResult.EXCEPTION);
    }

    @Bean
    public ProcessSource processSuccessSource(
        @Autowired @Qualifier("processSuccess") TestProcessFactory f) {
      return new TestProcessSource(f.getPipelineName(), f.processCnt);
    }

    @Bean
    public ProcessSource processFailureSource(
        @Autowired @Qualifier("processFailure") TestProcessFactory f) {
      return new TestProcessSource(f.getPipelineName(), f.processCnt);
    }

    @Bean
    public ProcessSource processExceptionSource(
        @Autowired @Qualifier("processException") TestProcessFactory f) {
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
        stageConfiguration,
        lockService,
        processFactoryService,
        processSourceService,
        processService,
        stageService,
        mailService,
        meterRegistry,
        pipelineName);
  }

  @Value
  public static class TestProcessFactory implements ProcessFactory {
    private final String pipelineName;
    public final int processCnt;
    public final int stageCnt;
    public final StageTestResult stageTestResult;
    public final List<String> processIds = Collections.synchronizedList(new ArrayList<>());
    public final AtomicLong stageExecCnt = new AtomicLong();

    public TestProcessFactory(
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
    public int getProcessParallelism() {
      return 5;
    }

    @Override
    public Process create(String processId) {
      processIds.add(processId);
      StageExecutorParameters executorParams =
          StageExecutorParameters.builder().immediateRetries(0).maximumRetries(0).build();

      ProcessBuilder processBuilder = new ProcessBuilder(processId);
      for (int i = 0; i < stageCnt; ++i) {
        processBuilder
            .execute("STAGE" + i, executorParams)
            .with(
                (pipelineName, processId1, stage) -> {
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
                });
      }
      return processBuilder.build();
    }
  }

  private void assertLauncherStats(PipeliteLauncher pipeliteLauncher, TestProcessFactory f) {
    ProcessRunnerStats stats = pipeliteLauncher.getStats();

    assertThat(stats.getProcessCreationFailedCount()).isEqualTo(0);
    assertThat(stats.getProcessExceptionCount()).isEqualTo(0);

    if (f.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(stats.getFailedProcessCount()).isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(stats.getFailedStageCount()).isEqualTo(f.stageExecCnt.get());
      assertThat(stats.getSuccessfulStageCount()).isEqualTo(0L);

    } else {
      assertThat(stats.getCompletedProcessCount()).isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(stats.getFailedStageCount()).isEqualTo(0L);
      assertThat(stats.getSuccessfulStageCount()).isEqualTo(f.stageExecCnt.get());
    }
  }

  private void assertProcessEntity(TestProcessFactory f, String processId) {
    String pipelineName = f.getPipelineName();

    ProcessEntity processEntity =
        processService.getSavedProcess(f.getPipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    if (f.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(processEntity.getState())
          .isEqualTo(ProcessState.FAILED); // no re-executions allowed
    } else {
      assertThat(processEntity.getState()).isEqualTo(ProcessState.COMPLETED);
    }
  }

  private void assertStageEntities(TestProcessFactory f, String processId) {
    String pipelineName = f.getPipelineName();

    for (int i = 0; i < f.stageCnt; ++i) {
      StageEntity stageEntity =
          stageService.getSavedStage(f.getPipelineName(), processId, "STAGE" + i).get();
      assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
      assertThat(stageEntity.getProcessId()).isEqualTo(processId);
      assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
      assertThat(stageEntity.getStartTime()).isNotNull();
      assertThat(stageEntity.getEndTime()).isNotNull();
      assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
      assertThat(stageEntity.getExecutorName()).startsWith(PipeliteLauncherTester.class.getName());
      assertThat(stageEntity.getExecutorData()).isEqualTo("{ }");
      assertThat(stageEntity.getExecutorParams())
          .isEqualTo("{\n  \"maximumRetries\" : 0,\n  \"immediateRetries\" : 0\n}");

      if (f.stageTestResult == StageTestResult.ERROR) {
        assertThat(stageEntity.getResultType()).isEqualTo(StageExecutorResultType.ERROR);
        assertThat(stageEntity.getResultParams()).isNull();
      } else if (f.stageTestResult == StageTestResult.EXCEPTION) {
        assertThat(stageEntity.getResultType()).isEqualTo(StageExecutorResultType.ERROR);
        assertThat(stageEntity.getResultParams())
            .contains("exception\" : \"java.lang.RuntimeException: Expected exception");
      } else {
        assertThat(stageEntity.getResultType()).isEqualTo(StageExecutorResultType.SUCCESS);
        assertThat(stageEntity.getResultParams()).isNull();
      }
    }
  }

  private void test(TestProcessFactory f) {
    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    assertThat(pipeliteLauncher.getActiveProcessRunners().size()).isEqualTo(0);

    assertThat(f.stageExecCnt.get() / f.stageCnt).isEqualTo(f.processCnt);
    assertThat(f.processIds.size()).isEqualTo(f.processCnt);
    assertLauncherStats(pipeliteLauncher, f);
    for (String processId : f.processIds) {
      assertProcessEntity(f, processId);
      assertStageEntities(f, processId);
    }
  }

  public void testSuccess() {
    test(processSuccess);
  }

  public void testFailure() {
    test(processFailure);
  }

  public void testException() {
    test(processException);
  }
}
