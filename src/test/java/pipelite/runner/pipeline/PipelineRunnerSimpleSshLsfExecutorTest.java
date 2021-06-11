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

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.helper.CreateProcessSingleStageSimpleLsfPipelineTestHelper;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.ProcessState;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;
import pipelite.stage.StageState;
import pipelite.stage.executor.ErrorType;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "PipelineRunnerSimpleSshLsfExecutorTest"})
@DirtiesContext
public class PipelineRunnerSimpleSshLsfExecutorTest {

  private static final int PROCESS_CNT = 2;
  private static final int RETRY_CNT = 3;
  private static final int PARALLELISM = 1;

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private RunnerService runnerService;
  @Autowired private PipeliteMetrics metrics;

  @Autowired private List<PipelineRunnerSimpleSshLsfExecutorTest.TestPipeline> testPipelines;

  @Profile("PipelineRunnerSimpleSshLsfExecutorTest")
  @TestConfiguration
  static class TestConfig {
    @Autowired private LsfTestConfiguration lsfTestConfiguration;

    @Bean
    public SuccessPipeline successPipeline() {
      return new SuccessPipeline(lsfTestConfiguration);
    }

    @Bean
    public NonPermanentErrorPipeline nonPermanentErrorPipeline() {
      return new NonPermanentErrorPipeline(lsfTestConfiguration);
    }

    @Bean
    public PermanentErrorPipeline permanentErrorPipeline() {
      return new PermanentErrorPipeline(lsfTestConfiguration);
    }
  }

  protected static class TestPipeline extends CreateProcessSingleStageSimpleLsfPipelineTestHelper {
    public TestPipeline(int exitCode, LsfTestConfiguration lsfTestConfiguration) {
      super(PROCESS_CNT, exitCode, PARALLELISM, RETRY_CNT, RETRY_CNT, lsfTestConfiguration);
    }
  }

  protected static class SuccessPipeline extends TestPipeline {
    public SuccessPipeline(LsfTestConfiguration lsfTestConfiguration) {
      super(0, lsfTestConfiguration);
    }
  }

  protected static class NonPermanentErrorPipeline extends TestPipeline {
    public NonPermanentErrorPipeline(LsfTestConfiguration lsfTestConfiguration) {
      super(1, lsfTestConfiguration);
    }
  }

  protected static class PermanentErrorPipeline extends TestPipeline {
    public PermanentErrorPipeline(LsfTestConfiguration lsfTestConfiguration) {
      super(0, lsfTestConfiguration);
    }

    @Override
    protected void testExecutorParams(
        SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?>
            executorParamsBuilder) {
      executorParamsBuilder.permanentError(0);
    }
  }

  private boolean isExpectedError(TestPipeline testPipeline) {
    return testPipeline instanceof NonPermanentErrorPipeline
        || testPipeline instanceof PermanentErrorPipeline;
  }

  private boolean isExpectedNonPermanentError(TestPipeline testPipeline) {
    return testPipeline instanceof NonPermanentErrorPipeline;
  }

  private boolean isExpectedPermanentError(TestPipeline testPipeline) {
    return testPipeline instanceof PermanentErrorPipeline;
  }

  private void assertMetrics(TestPipeline f) {
    String pipelineName = f.pipelineName();

    PipelineMetrics pipelineMetrics = metrics.pipeline(pipelineName);

    if (isExpectedPermanentError(f)) {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0L);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(0);
    } else if (isExpectedNonPermanentError(f)) {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT * (1 + RETRY_CNT));
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0L);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(PROCESS_CNT * (1 + RETRY_CNT));
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(0);
    } else {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(PROCESS_CNT);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(PROCESS_CNT);
    }
  }

  private void assertProcessEntity(TestPipeline f, String processId) {
    String pipelineName = f.pipelineName();

    ProcessEntity processEntity = processService.getSavedProcess(f.pipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    if (!isExpectedError(f)) {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    } else {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);
    }
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isAfter(processEntity.getStartTime());
  }

  private void assertStageEntity(TestPipeline f, String processId) {
    String pipelineName = f.pipelineName();

    StageEntity stageEntity =
        stageService.getSavedStage(f.pipelineName(), processId, f.stageName()).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    if (isExpectedNonPermanentError(f)) {
      assertThat(stageEntity.getExecutionCount()).isEqualTo(RETRY_CNT + 1);
    } else {
      assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    }
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isAfter(stageEntity.getStartTime());
    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SimpleLsfExecutor");
    assertThat(stageEntity.getExecutorData()).contains("  \"cmd\" : \"" + f.cmd() + "\"");
    assertThat(stageEntity.getExecutorData()).contains("  \"jobId\" : \"");
    assertThat(stageEntity.getExecutorData()).contains("  \"outFile\" : \"");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 180000,\n"
                + "  \"maximumRetries\" : "
                + RETRY_CNT
                + ",\n"
                + "  \"immediateRetries\" : "
                + RETRY_CNT
                + ",\n"
                + "  \"host\" : \"noah-login\",\n"
                + "  \"workDir\" : \"pipelite-test\",\n"
                + (isExpectedPermanentError(f)
                    ? "  \"logBytes\" : 1048576,\n"
                        + "  \"permanentErrors\" : [ "
                        + f.exitCode()
                        + " ]\n"
                    : "  \"logBytes\" : 1048576\n")
                + "}");
    if (isExpectedError(f)) {
      assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
    } else {
      assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
    }
    if (isExpectedPermanentError(f)) {
      assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.PERMANENT_ERROR);
    } else if (isExpectedNonPermanentError(f)) {
      assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.EXECUTION_ERROR);
    } else {
      assertThat(stageEntity.getErrorType()).isNull();
    }
    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + f.exitCode() + "\"");
    assertThat(stageEntity.getResultParams()).contains("\"job id\" :");
  }

  private void assertPipeline(TestPipeline f) {

    for (PipelineRunner pipelineRunner : runnerService.getPipelineRunners()) {
      assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);
    }
    assertThat(f.configuredProcessIds().size()).isEqualTo(PROCESS_CNT);
    assertMetrics(f);
    for (String processId : f.configuredProcessIds()) {
      assertProcessEntity(f, processId);
      assertStageEntity(f, processId);
    }
  }

  @Test
  public void runSchedules() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    for (TestPipeline f : testPipelines) {
      assertPipeline(f);
    }
  }
}
