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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import org.springframework.test.context.ContextConfiguration;
import pipelite.*;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.*;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ContextConfiguration(initializers = PipeliteTestConfiguration.TestContextInitializer.class)
@DirtiesContext
public class PipeliteLauncherFailureTest {

  private static final int PROCESS_CNT = 1;

  @Autowired private ServiceConfiguration serviceConfiguration;
  @Autowired private AdvancedConfiguration advancedConfiguration;
  @Autowired private ExecutorConfiguration executorConfiguration;
  @Autowired private RegisteredPipelineService registeredPipelineService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private PipeliteLockerService pipeliteLockerService;
  @Autowired private MailService mailService;
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

  private PipeliteLauncher createPipeliteLauncher(String pipelineName) {
    return DefaultPipeliteLauncher.create(
        serviceConfiguration,
        advancedConfiguration,
        executorConfiguration,
        pipeliteLockerService.getPipeliteLocker(),
        registeredPipelineService,
        processService,
        stageService,
        mailService,
        metrics,
        pipelineName);
  }

  @Value
  public static class TestPipeline implements PrioritizedPipeline {
    private final String pipelineName =
        UniqueStringGenerator.randomPipelineName(PipeliteLauncherFailureTest.class);
    public final PrioritizedPipelineTestHelper helper =
        new PrioritizedPipelineTestHelper(PROCESS_CNT);
    private final StageTestResult stageTestResult;
    public final List<String> processIds = Collections.synchronizedList(new ArrayList<>());
    private StageExecutorResult firstStageExecResult;
    private StageExecutorResult secondStageExecResult;
    private StageExecutorResult thirdStageExecResult;
    private StageExecutorResult fourthStageExecResult;
    public final AtomicLong firstStageExecCnt = new AtomicLong();
    public final AtomicLong secondStageExecCnt = new AtomicLong();
    public final AtomicLong thirdStageExecCnt = new AtomicLong();
    public final AtomicLong fourthStageExecCnt = new AtomicLong();

    public TestPipeline(StageTestResult stageTestResult) {
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

      return builder
          .execute("STAGE0")
          .withCallExecutor(
              (pipelineName1) -> {
                firstStageExecCnt.incrementAndGet();
                return firstStageExecResult;
              },
              executorParams)
          .executeAfterPrevious("STAGE1")
          .withCallExecutor(
              (pipelineName1) -> {
                secondStageExecCnt.incrementAndGet();
                return secondStageExecResult;
              },
              executorParams)
          .executeAfterPrevious("STAGE2")
          .withCallExecutor(
              (pipelineName1) -> {
                thirdStageExecCnt.incrementAndGet();
                return thirdStageExecResult;
              },
              executorParams)
          .executeAfterPrevious("STAGE3")
          .withCallExecutor(
              (pipelineName1) -> {
                fourthStageExecCnt.incrementAndGet();
                return fourthStageExecResult;
              },
              executorParams)
          .build();
    }

    @Override
    public NextProcess nextProcess() {
      return helper.nextProcess();
    }

    @Override
    public void confirmProcess(String processId) {
      helper.confirmProcess(processId);
    }
  }

  private PipeliteLauncher pipeliteLauncher(String pipelineName) {
    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(pipelineName);
    return pipeliteLauncher;
  }

  public void test(TestPipeline testPipeline) {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncher(testPipeline.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    assertThat(pipeliteLauncher.getActiveProcessRunners().size()).isEqualTo(0);

    assertThat(testPipeline.helper.getNewProcesses()).isEqualTo(0);
    assertThat(testPipeline.helper.getReturnedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(testPipeline.helper.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(testPipeline.helper.getRejectedProcesses()).isEqualTo(0);

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

    assertThat(testPipeline.processIds.size()).isEqualTo(PROCESS_CNT);
    assertLauncherMetrics(testPipeline);
    for (String processId : testPipeline.processIds) {
      assertProcessEntity(testPipeline, processId);
      assertStageEntities(testPipeline, processId);
    }
  }

  private void assertProcessEntity(TestPipeline f, String processId) {
    String pipelineName = f.getPipelineName();

    ProcessEntity processEntity =
        processService.getSavedProcess(f.getPipelineName(), processId).get();
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
    String pipelineName = f.getPipelineName();

    int stageCnt = 4;
    for (int i = 0; i < stageCnt; ++i) {
      StageEntity stageEntity =
          stageService.getSavedStage(f.getPipelineName(), processId, "STAGE" + i).get();

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
        assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.CallExecutor");
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

  private void assertLauncherMetrics(TestPipeline f) {
    PipelineMetrics pipelineMetrics = metrics.pipeline(f.getPipelineName());
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
  public void testFirstStageFails() {
    test(firstStageFails);
  }

  @Test
  public void testSecondStageFails() {
    test(secondStageFails);
  }

  @Test
  public void testThirdStageFails() {
    test(thirdStageFails);
  }

  @Test
  public void testFourthStageFails() {
    test(fourthStageFails);
  }

  @Test
  public void testNoStageFails() {
    test(noStageFails);
  }
}
