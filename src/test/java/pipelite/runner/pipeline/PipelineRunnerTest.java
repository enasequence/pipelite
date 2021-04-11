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
package pipelite.runner.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.entity.StageLogEntity;
import pipelite.helper.PrioritizedPipelineTestHelper;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "PipelineRunnerTest"})
@DirtiesContext
public class PipelineRunnerTest {

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics pipeliteMetrics;

  @Autowired
  @Qualifier("successPipeline")
  private TestPipeline successPipeline;

  @Autowired
  @Qualifier("failurePipeline")
  private TestPipeline failurePipeline;

  @Autowired
  @Qualifier("exceptionPipeline")
  private TestPipeline exceptionPipeline;

  @Profile("PipelineRunnerTest")
  @TestConfiguration
  static class TestConfig {
    @Bean("successPipeline")
    @Primary
    public TestPipeline successPipeline() {
      return new TestPipeline(4, 2, StageTestResult.SUCCESS);
    }

    @Bean("failurePipeline")
    public TestPipeline failurePipeline() {
      return new TestPipeline(4, 2, StageTestResult.ERROR);
    }

    @Bean("exceptionPipeline")
    public TestPipeline exceptionPipeline() {
      return new TestPipeline(4, 2, StageTestResult.EXCEPTION);
    }
  }

  private enum StageTestResult {
    SUCCESS,
    ERROR,
    EXCEPTION
  }

  @Accessors(fluent = true)
  @Getter
  public static class TestPipeline extends PrioritizedPipelineTestHelper {
    private final int stageCnt;
    private final StageTestResult stageTestResult;
    private final AtomicLong stageExecCnt = new AtomicLong();

    public TestPipeline(int processCnt, int stageCnt, StageTestResult stageTestResult) {
      super(processCnt);
      this.stageCnt = stageCnt;
      this.stageTestResult = stageTestResult;
    }

    @Override
    public int _configureParallelism() {
      return 5;
    }

    @Override
    public void _configureProcess(ProcessBuilder builder) {
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
    }
  }

  private void assertMetrics(TestPipeline f) {
    PipelineMetrics metrics = pipeliteMetrics.pipeline(f.pipelineName());

    if (f.stageTestResult() != StageTestResult.SUCCESS) {
      assertThat(metrics.process().getFailedCount())
          .isEqualTo(f.stageExecCnt().get() / f.stageCnt());
      assertThat(metrics.stage().getFailedCount()).isEqualTo(f.stageExecCnt().get());
      assertThat(metrics.stage().getSuccessCount()).isEqualTo(0L);
      assertThat(TimeSeriesMetrics.getCount(metrics.process().getFailedTimeSeries()))
          .isEqualTo(f.stageExecCnt().get() / f.stageCnt());
      assertThat(TimeSeriesMetrics.getCount(metrics.stage().getFailedTimeSeries()))
          .isEqualTo(f.stageExecCnt().get());
      assertThat(TimeSeriesMetrics.getCount(metrics.stage().getSuccessTimeSeries())).isEqualTo(0);
    } else {
      assertThat(metrics.process().getCompletedCount())
          .isEqualTo(f.stageExecCnt().get() / f.stageCnt());
      assertThat(metrics.stage().getFailedCount()).isEqualTo(0L);
      assertThat(metrics.stage().getSuccessCount()).isEqualTo(f.stageExecCnt().get());
      assertThat(TimeSeriesMetrics.getCount(metrics.process().getCompletedTimeSeries()))
          .isEqualTo(f.stageExecCnt().get() / f.stageCnt());
      assertThat(TimeSeriesMetrics.getCount(metrics.stage().getFailedTimeSeries())).isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(metrics.stage().getSuccessTimeSeries()))
          .isEqualTo(f.stageExecCnt().get());
    }
  }

  private void assertProcessEntity(TestPipeline f, String processId) {
    String pipelineName = f.pipelineName();

    ProcessEntity processEntity =
        pipeliteServices.process().getSavedProcess(f.pipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    if (f.stageTestResult() != StageTestResult.SUCCESS) {
      assertThat(processEntity.getProcessState())
          .isEqualTo(ProcessState.FAILED); // no re-executions allowed
    } else {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    }
  }

  private void assertStageEntities(TestPipeline f, String processId) {
    String pipelineName = f.pipelineName();

    for (int i = 0; i < f.stageCnt(); ++i) {
      StageEntity stageEntity =
          pipeliteServices.stage().getSavedStage(f.pipelineName(), processId, "STAGE" + i).get();
      StageLogEntity stageLogEntity =
          pipeliteServices.stage().getSavedStageLog(f.pipelineName(), processId, "STAGE" + i).get();
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

      if (f.stageTestResult() == StageTestResult.ERROR) {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
        assertThat(stageEntity.getResultParams()).isNull();
      } else if (f.stageTestResult() == StageTestResult.EXCEPTION) {
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

  private void assertPipeline(TestPipeline f) {
    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);
    assertThat(f.processCnt()).isGreaterThan(0);
    assertThat(f.stageCnt()).isGreaterThan(0);
    assertThat(f.stageExecCnt().get() / f.stageCnt()).isEqualTo(f.processCnt());
    assertThat(f.configuredProcessCount()).isEqualTo(f.processCnt());
    assertMetrics(f);
    for (String processId : f.configuredProcessIds()) {
      assertProcessEntity(f, processId);
      assertStageEntities(f, processId);
    }
  }

  @Test
  public void testPipelines() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    assertPipeline(successPipeline);
    assertPipeline(failurePipeline);
    assertPipeline(exceptionPipeline);
  }
}
