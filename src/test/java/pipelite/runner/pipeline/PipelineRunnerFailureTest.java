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
import pipelite.helper.CreateProcessPipelineTestHelper;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerFailureTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "PipelineRunnerFailureTest"})
@DirtiesContext
public class PipelineRunnerFailureTest {

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
  public static class TestPipeline extends CreateProcessPipelineTestHelper {
    private final StageTestResult stageTestResult;
    private StageExecutorResult firstStageExecResult;
    private StageExecutorResult secondStageExecResult;
    private StageExecutorResult thirdStageExecResult;
    private StageExecutorResult fourthStageExecResult;
    public final AtomicLong firstStageExecCnt = new AtomicLong();
    public final AtomicLong secondStageExecCnt = new AtomicLong();
    public final AtomicLong thirdStageExecCnt = new AtomicLong();
    public final AtomicLong fourthStageExecCnt = new AtomicLong();

    public TestPipeline(StageTestResult stageTestResult) {
      super(PROCESS_CNT);
      this.stageTestResult = stageTestResult;
      this.firstStageExecResult =
          stageTestResult == StageTestResult.FIRST_ERROR
              ? StageExecutorResult.error()
              : StageExecutorResult.success();
      this.secondStageExecResult =
          stageTestResult == StageTestResult.SECOND_ERROR
              ? StageExecutorResult.error()
              : StageExecutorResult.success();
      this.thirdStageExecResult =
          stageTestResult == StageTestResult.THIRD_ERROR
              ? StageExecutorResult.error()
              : StageExecutorResult.success();
      this.fourthStageExecResult =
          stageTestResult == StageTestResult.FOURTH_ERROR
              ? StageExecutorResult.error()
              : StageExecutorResult.success();
    }

    @Override
    public int testConfigureParallelism() {
      return 5;
    }

    @Override
    public void testConfigureProcess(ProcessBuilder builder) {
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

  public void assertPipeline(TestPipeline testPipeline) {

    PipelineRunner pipelineRunner =
        runnerService.getPipelineRunner(testPipeline.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    assertThat(testPipeline.createdProcessCount()).isEqualTo(0);
    assertThat(testPipeline.returnedProcessCount()).isEqualTo(PROCESS_CNT);
    assertThat(testPipeline.confirmedProcessCount()).isEqualTo(PROCESS_CNT);

    if (testPipeline.stageTestResult == StageTestResult.FIRST_ERROR) {
      assertThat(testPipeline.firstStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(testPipeline.secondStageExecCnt.get()).isEqualTo(0);
      assertThat(testPipeline.thirdStageExecCnt.get()).isEqualTo(0);
      assertThat(testPipeline.fourthStageExecCnt.get()).isEqualTo(0);
    } else if (testPipeline.stageTestResult == StageTestResult.SECOND_ERROR) {
      assertThat(testPipeline.firstStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(testPipeline.secondStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(testPipeline.thirdStageExecCnt.get()).isEqualTo(0);
      assertThat(testPipeline.fourthStageExecCnt.get()).isEqualTo(0);
    } else if (testPipeline.stageTestResult == StageTestResult.THIRD_ERROR) {
      assertThat(testPipeline.firstStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(testPipeline.secondStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(testPipeline.thirdStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(testPipeline.fourthStageExecCnt.get()).isEqualTo(0);
    } else {
      assertThat(testPipeline.firstStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(testPipeline.secondStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(testPipeline.thirdStageExecCnt.get()).isEqualTo(PROCESS_CNT);
      assertThat(testPipeline.fourthStageExecCnt.get()).isEqualTo(PROCESS_CNT);
    }

    assertThat(testPipeline.configuredProcessCount()).isEqualTo(PROCESS_CNT);
    assertMetrics(testPipeline);
    for (String processId : testPipeline.configuredProcessIds()) {
      assertProcessEntity(testPipeline, processId);
      assertStageEntities(testPipeline, processId);
    }
  }

  private void assertProcessEntity(TestPipeline f, String processId) {
    String pipelineName = f.pipelineName();

    ProcessEntity processEntity = processService.getSavedProcess(f.pipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    if (f.stageTestResult == StageTestResult.NO_ERROR) {
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

      if ((i > 0 && f.stageTestResult == StageTestResult.FIRST_ERROR)
          || (i > 1 && f.stageTestResult == StageTestResult.SECOND_ERROR)
          || (i > 2 && f.stageTestResult == StageTestResult.THIRD_ERROR)
          || (i > 3 && f.stageTestResult == StageTestResult.FOURTH_ERROR)) {

        // Stage has been created but not executed.

        assertThat(stageEntity.getExecutionCount()).isZero();
        assertThat(stageEntity.getStartTime()).isNull();
        assertThat(stageEntity.getEndTime()).isNull();
        assertThat(stageEntity.getExecutorName()).isNull();
        assertThat(stageEntity.getExecutorData()).isNull();
        assertThat(stageEntity.getExecutorParams()).isNull();
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
        assertThat(stageEntity.getResultParams()).isNull();
      } else {

        // Stage has been executed.

        assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
        assertThat(stageEntity.getStartTime()).isNotNull();
        assertThat(stageEntity.getEndTime()).isNotNull();
        assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
        assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.TestExecutor");
        assertThat(stageEntity.getExecutorData()).isNull();
        assertThat(stageEntity.getExecutorParams())
            .isEqualTo(
                "{\n"
                    + "  \"timeout\" : 10000,\n"
                    + "  \"maximumRetries\" : 0,\n"
                    + "  \"immediateRetries\" : 0\n"
                    + "}");

        if ((i == 0 && f.stageTestResult == StageTestResult.FIRST_ERROR)
            || (i == 1 && f.stageTestResult == StageTestResult.SECOND_ERROR)
            || (i == 2 && f.stageTestResult == StageTestResult.THIRD_ERROR)
            || (i == 3 && f.stageTestResult == StageTestResult.FOURTH_ERROR)) {
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
    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());
    if (f.getFirstStageExecResult().isSuccess()
        && f.getSecondStageExecResult().isSuccess()
        && f.getThirdStageExecResult().isSuccess()
        && f.getFourthStageExecResult().isSuccess()) {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(PROCESS_CNT);
      assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(0);
    } else {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(0);
      assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    }

    int stageSuccessCount = 0;
    int stageFailedCount = PROCESS_CNT;
    if (f.getFirstStageExecResult().isSuccess()) {
      stageSuccessCount += PROCESS_CNT;
      if (f.getSecondStageExecResult().isSuccess()) {
        stageSuccessCount += PROCESS_CNT;
        if (f.getThirdStageExecResult().isSuccess()) {
          stageSuccessCount += PROCESS_CNT;
          if (f.getFourthStageExecResult().isSuccess()) {
            stageSuccessCount += PROCESS_CNT;
            stageFailedCount = 0;
          }
        }
      }
    }
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(stageSuccessCount);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(stageFailedCount);
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
