/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.entity.field.StageState;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.collector.ProcessRunnerMetrics;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.test.configuration.PipeliteTestConfigWithManager;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerFailureTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"pipelite", "PipelineRunnerFailureTest"})
@DirtiesContext
public class PipelineRunnerFailureTest {

  private static final int PARALLELISM = 5;
  private static final int PROCESS_CNT = 1;

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private RunnerService runnerService;
  @Autowired private PipeliteMetrics metrics;

  @Autowired
  @Qualifier("firstStageFails")
  private TestPipeline firstStageFails;

  @Autowired
  @Qualifier("secondStageFails")
  private TestPipeline secondStageFails;

  @Autowired
  @Qualifier("thirdStageFails")
  private TestPipeline thirdStageFails;

  @Autowired
  @Qualifier("fourthStageFails")
  private TestPipeline fourthStageFails;

  @Autowired
  @Qualifier("noStageFails")
  private TestPipeline noStageFails;

  @Profile("PipelineRunnerFailureTest")
  @TestConfiguration
  static class TestConfig {
    @Bean("firstStageFails")
    @Primary
    public TestPipeline firstStageFails() {
      return new TestPipeline(StageTestResult.FIRST_ERROR);
    }

    @Bean("secondStageFails")
    public TestPipeline secondStageFails() {
      return new TestPipeline(StageTestResult.SECOND_ERROR);
    }

    @Bean("thirdStageFails")
    public TestPipeline thirdStageFails() {
      return new TestPipeline(StageTestResult.THIRD_ERROR);
    }

    @Bean("fourthStageFails")
    public TestPipeline fourthStageFails() {
      return new TestPipeline(StageTestResult.FOURTH_ERROR);
    }

    @Bean("noStageFails")
    public TestPipeline noStageFails() {
      return new TestPipeline(StageTestResult.NO_ERROR);
    }
  }

  private enum StageTestResult {
    FIRST_ERROR,
    SECOND_ERROR,
    THIRD_ERROR,
    FOURTH_ERROR,
    NO_ERROR
  }

  @Getter
  private static class TestProcess extends TestProcessConfiguration {
    private final StageTestResult stageTestResult;
    private StageExecutorResult firstStageExecResult;
    private StageExecutorResult secondStageExecResult;
    private StageExecutorResult thirdStageExecResult;
    private StageExecutorResult fourthStageExecResult;
    public final AtomicLong firstStageExecCnt = new AtomicLong();
    public final AtomicLong secondStageExecCnt = new AtomicLong();
    public final AtomicLong thirdStageExecCnt = new AtomicLong();
    public final AtomicLong fourthStageExecCnt = new AtomicLong();

    public TestProcess(StageTestResult stageTestResult) {
      this.stageTestResult = stageTestResult;
      this.firstStageExecResult =
          stageTestResult == StageTestResult.FIRST_ERROR
              ? StageExecutorResult.executionError()
              : StageExecutorResult.success();
      this.secondStageExecResult =
          stageTestResult == StageTestResult.SECOND_ERROR
              ? StageExecutorResult.executionError()
              : StageExecutorResult.success();
      this.thirdStageExecResult =
          stageTestResult == StageTestResult.THIRD_ERROR
              ? StageExecutorResult.executionError()
              : StageExecutorResult.success();
      this.fourthStageExecResult =
          stageTestResult == StageTestResult.FOURTH_ERROR
              ? StageExecutorResult.executionError()
              : StageExecutorResult.success();
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {
      ExecutorParameters executorParams =
          ExecutorParameters.builder()
              .immediateRetries(0)
              .maximumRetries(0)
              .timeout(Duration.ofSeconds(10))
              .build();

      builder
          .execute("STAGE0")
          .withSyncTestExecutor(
              (pipelineName1) -> {
                firstStageExecCnt.incrementAndGet();
                return firstStageExecResult;
              },
              executorParams)
          .executeAfterPrevious("STAGE1")
          .withSyncTestExecutor(
              (pipelineName1) -> {
                secondStageExecCnt.incrementAndGet();
                return secondStageExecResult;
              },
              executorParams)
          .executeAfterPrevious("STAGE2")
          .withSyncTestExecutor(
              (pipelineName1) -> {
                thirdStageExecCnt.incrementAndGet();
                return thirdStageExecResult;
              },
              executorParams)
          .executeAfterPrevious("STAGE3")
          .withSyncTestExecutor(
              (pipelineName1) -> {
                fourthStageExecCnt.incrementAndGet();
                return fourthStageExecResult;
              },
              executorParams);
    }
  }

  @Getter
  private static class TestPipeline extends ConfigurableTestPipeline<TestProcess> {
    public TestPipeline(StageTestResult stageTestResult) {
      super(PARALLELISM, PROCESS_CNT, new TestProcess(stageTestResult));
    }
  }

  public void assertPipeline(TestPipeline f) {

    PipelineRunner pipelineRunner = runnerService.getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    assertThat(f.createdProcessCount()).isEqualTo(0);
    assertThat(f.returnedProcessCount()).isEqualTo(PROCESS_CNT);
    assertThat(f.confirmedProcessCount()).isEqualTo(PROCESS_CNT);

    TestProcess t = f.testProcessConfiguration();
    if (t.stageTestResult == StageTestResult.FIRST_ERROR) {
      assertThat(t.firstStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(t.secondStageExecCnt.get()).isEqualTo(0);
      assertThat(t.thirdStageExecCnt.get()).isEqualTo(0);
      assertThat(t.fourthStageExecCnt.get()).isEqualTo(0);
    } else if (t.stageTestResult == StageTestResult.SECOND_ERROR) {
      assertThat(t.firstStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(t.secondStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(t.thirdStageExecCnt.get()).isEqualTo(0);
      assertThat(t.fourthStageExecCnt.get()).isEqualTo(0);
    } else if (t.stageTestResult == StageTestResult.THIRD_ERROR) {
      assertThat(t.firstStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(t.secondStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(t.thirdStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(t.fourthStageExecCnt.get()).isEqualTo(0);
    } else {
      assertThat(t.firstStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(t.secondStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(t.thirdStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(t.fourthStageExecCnt.get()).isEqualTo(PROCESS_CNT);
    }

    assertThat(f.configuredProcessCount()).isEqualTo(PROCESS_CNT);
    assertMetrics(f);
    for (String processId : f.configuredProcessIds()) {
      assertProcessEntity(f, processId);
      assertStageEntities(f, processId);
    }
  }

  private void assertProcessEntity(TestPipeline f, String processId) {
    String pipelineName = f.pipelineName();

    ProcessEntity processEntity = processService.getSavedProcess(f.pipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);

    TestProcess t = f.testProcessConfiguration();
    if (t.stageTestResult == StageTestResult.NO_ERROR) {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    } else {
      assertThat(processEntity.getProcessState())
          .isEqualTo(ProcessState.FAILED); // no re-executions allowed
    }
  }

  private void assertStageEntities(TestPipeline f, String processId) {
    String pipelineName = f.pipelineName();

    int stageCnt = 4;
    for (int i = 0; i < stageCnt; ++i) {
      StageEntity stageEntity =
          stageService.getSavedStage(f.pipelineName(), processId, "STAGE" + i).get();

      assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
      assertThat(stageEntity.getProcessId()).isEqualTo(processId);

      TestProcess t = f.testProcessConfiguration();
      if ((i > 0 && t.stageTestResult == StageTestResult.FIRST_ERROR)
          || (i > 1 && t.stageTestResult == StageTestResult.SECOND_ERROR)
          || (i > 2 && t.stageTestResult == StageTestResult.THIRD_ERROR)
          || (i > 3 && t.stageTestResult == StageTestResult.FOURTH_ERROR)) {

        // Stage has been created but not executed.

        assertThat(stageEntity.getExecutionCount()).isZero();
        assertThat(stageEntity.getStartTime()).isNull();
        assertThat(stageEntity.getEndTime()).isNull();
        assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SyncTestExecutor");
        assertThat(stageEntity.getExecutorData()).isNull();
        assertThat(stageEntity.getExecutorParams())
            .isEqualTo(
                "{\n"
                    + "  \"timeout\" : 10000,\n"
                    + "  \"maximumRetries\" : 0,\n"
                    + "  \"immediateRetries\" : 0,\n"
                    + "  \"logSave\" : \"ERROR\",\n"
                    + "  \"logLines\" : 1000\n"
                    + "}");
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
        assertThat(stageEntity.getResultParams()).isNull();
      } else {

        // Stage has been executed.

        assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
        assertThat(stageEntity.getStartTime()).isNotNull();
        assertThat(stageEntity.getEndTime()).isNotNull();
        assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
        assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SyncTestExecutor");
        assertThat(stageEntity.getExecutorData()).isNull();
        assertThat(stageEntity.getExecutorParams())
            .isEqualTo(
                "{\n"
                    + "  \"timeout\" : 10000,\n"
                    + "  \"maximumRetries\" : 0,\n"
                    + "  \"immediateRetries\" : 0,\n"
                    + "  \"logSave\" : \"ERROR\",\n"
                    + "  \"logLines\" : 1000\n"
                    + "}");

        if ((i == 0 && t.stageTestResult == StageTestResult.FIRST_ERROR)
            || (i == 1 && t.stageTestResult == StageTestResult.SECOND_ERROR)
            || (i == 2 && t.stageTestResult == StageTestResult.THIRD_ERROR)
            || (i == 3 && t.stageTestResult == StageTestResult.FOURTH_ERROR)) {
          assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
          assertThat(stageEntity.getResultParams()).isNull();
        } else {
          assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
          assertThat(stageEntity.getResultParams()).isNull();
        }
      }
    }
  }

  private void assertMetrics(TestPipeline f) {
    ProcessRunnerMetrics processRunnerMetrics = metrics.process(f.pipelineName());
    TestProcess t = f.testProcessConfiguration();
    if (t.getFirstStageExecResult().isSuccess()
        && t.getSecondStageExecResult().isSuccess()
        && t.getThirdStageExecResult().isSuccess()
        && t.getFourthStageExecResult().isSuccess()) {
      assertThat(processRunnerMetrics.completedCount()).isEqualTo(PROCESS_CNT);
      assertThat(processRunnerMetrics.failedCount()).isEqualTo(0);
    } else {
      assertThat(processRunnerMetrics.completedCount()).isEqualTo(0);
      assertThat(processRunnerMetrics.failedCount()).isEqualTo(PROCESS_CNT);
    }

    int firstStageSuccessCount = 0;
    int secondStageSuccessCount = 0;
    int thirdStageSuccessCount = 0;
    int fourthStageSuccessCount = 0;

    int firstStageFailedCount = 0;
    int secondStageFailedCount = 0;
    int thirdStageFailedCount = 0;
    int fourthStageFailedCount = 0;

    if (t.getFirstStageExecResult().isSuccess()) {
      firstStageSuccessCount += PROCESS_CNT;
      if (t.getSecondStageExecResult().isSuccess()) {
        secondStageSuccessCount += PROCESS_CNT;
        if (t.getThirdStageExecResult().isSuccess()) {
          thirdStageSuccessCount += PROCESS_CNT;
          if (t.getFourthStageExecResult().isSuccess()) {
            fourthStageSuccessCount += PROCESS_CNT;
          } else {
            fourthStageFailedCount += PROCESS_CNT;
          }
        } else {
          thirdStageFailedCount += PROCESS_CNT;
        }
      } else {
        secondStageFailedCount += PROCESS_CNT;
      }
    } else {
      firstStageFailedCount += PROCESS_CNT;
    }

    assertThat(processRunnerMetrics.stage("STAGE0").successCount())
        .isEqualTo(firstStageSuccessCount);
    assertThat(processRunnerMetrics.stage("STAGE1").successCount())
        .isEqualTo(secondStageSuccessCount);
    assertThat(processRunnerMetrics.stage("STAGE2").successCount())
        .isEqualTo(thirdStageSuccessCount);
    assertThat(processRunnerMetrics.stage("STAGE3").successCount())
        .isEqualTo(fourthStageSuccessCount);

    assertThat(processRunnerMetrics.stage("STAGE0").failedCount()).isEqualTo(firstStageFailedCount);
    assertThat(processRunnerMetrics.stage("STAGE1").failedCount())
        .isEqualTo(secondStageFailedCount);
    assertThat(processRunnerMetrics.stage("STAGE2").failedCount()).isEqualTo(thirdStageFailedCount);
    assertThat(processRunnerMetrics.stage("STAGE3").failedCount())
        .isEqualTo(fourthStageFailedCount);
  }

  @Test
  public void testPipelines() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    assertPipeline(firstStageFails);
    assertPipeline(secondStageFails);
    assertPipeline(thirdStageFails);
    assertPipeline(fourthStageFails);
    assertPipeline(noStageFails);
  }
}
