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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Value;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.TestProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;
import pipelite.stage.StageParameters;

@Component
@Scope("prototype")
public class PipeliteLauncherFailureTester {

  private static final int PROCESS_CNT = 5;

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;

  @Autowired
  @Qualifier("firstStageFails")
  private TestProcessFactory firstStageFails;

  @Autowired
  @Qualifier("secondStageFails")
  private TestProcessFactory secondStageFails;

  @Autowired
  @Qualifier("thirdStageFails")
  private TestProcessFactory thirdStageFails;

  @Autowired
  @Qualifier("fourthStageFails")
  private TestProcessFactory fourthStageFails;

  @Autowired
  @Qualifier("noStageFails")
  private TestProcessFactory noStageFails;

  @Autowired
  @Qualifier("firstStageFailsSource")
  private TestProcessSource firstStageFailsSource;

  @Autowired
  @Qualifier("secondStageFailsSource")
  public TestProcessSource secondStageFailsSource;

  @Autowired
  @Qualifier("thirdStageFailsSource")
  public TestProcessSource thirdStageFailsSource;

  @Autowired
  @Qualifier("fourthStageFailsSource")
  public TestProcessSource fourthStageFailsSource;

  @Autowired
  @Qualifier("noStageFailsSource")
  public TestProcessSource noStageFailsSource;

  @TestConfiguration
  static class TestConfig {
    @Bean("firstStageFails")
    @Primary
    public TestProcessFactory firstStageFails() {
      return new TestProcessFactory(
          StageExecutionResult.error(),
          StageExecutionResult.success(),
          StageExecutionResult.success(),
          StageExecutionResult.success());
    }

    @Bean("secondStageFails")
    public ProcessFactory secondStageFails() {
      return new TestProcessFactory(
          StageExecutionResult.success(),
          StageExecutionResult.error(),
          StageExecutionResult.success(),
          StageExecutionResult.success());
    }

    @Bean("thirdStageFails")
    public ProcessFactory thirdStageFails() {
      return new TestProcessFactory(
          StageExecutionResult.success(),
          StageExecutionResult.success(),
          StageExecutionResult.error(),
          StageExecutionResult.success());
    }

    @Bean("fourthStageFails")
    public ProcessFactory fourthStageFails() {
      return new TestProcessFactory(
          StageExecutionResult.success(),
          StageExecutionResult.success(),
          StageExecutionResult.success(),
          StageExecutionResult.error());
    }

    @Bean("noStageFails")
    public ProcessFactory noStageFails() {
      return new TestProcessFactory(
          StageExecutionResult.success(),
          StageExecutionResult.success(),
          StageExecutionResult.success(),
          StageExecutionResult.error());
    }

    @Bean("firstStageFailsSource")
    @Primary
    public ProcessSource firstStageFailsSource(
        @Autowired @Qualifier("firstStageFails") TestProcessFactory f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean("secondStageFailsSource")
    public ProcessSource secondStageFailsSource(
        @Autowired @Qualifier("secondStageFails") TestProcessFactory f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean("thirdStageFailsSource")
    public ProcessSource thirdStageFailsSource(
        @Autowired @Qualifier("thirdStageFails") TestProcessFactory f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean("fourthStageFailsSource")
    public ProcessSource fourthStageFailsSource(
        @Autowired @Qualifier("fourthStageFails") TestProcessFactory f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean("noStageFailsSource")
    public ProcessSource noStageFailsSource(
        @Autowired @Qualifier("noStageFails") TestProcessFactory f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }
  }

  @Value
  public static class TestProcessFactory implements ProcessFactory {
    private final String pipelineName = UniqueStringGenerator.randomPipelineName();
    public final List<String> processIds = Collections.synchronizedList(new ArrayList<>());
    private StageExecutionResult firstStageExecResult;
    private StageExecutionResult secondStageExecResult;
    private StageExecutionResult thirdStageExecResult;
    private StageExecutionResult fourthStageExecResult;
    public final AtomicLong firstStageExecCnt = new AtomicLong();
    public final AtomicLong secondStageExecCnt = new AtomicLong();
    public final AtomicLong thirdStageExecCnt = new AtomicLong();
    public final AtomicLong fourthStageExecCnt = new AtomicLong();

    public TestProcessFactory(
        StageExecutionResult firstStageExecResult,
        StageExecutionResult secondStageExecResult,
        StageExecutionResult thirdStageExecResult,
        StageExecutionResult fourthStageExecResult) {
      this.firstStageExecResult = firstStageExecResult;
      this.secondStageExecResult = secondStageExecResult;
      this.thirdStageExecResult = thirdStageExecResult;
      this.fourthStageExecResult = fourthStageExecResult;
    }

    public void reset() {
      processIds.clear();
      firstStageExecCnt.set(0);
      secondStageExecCnt.set(0);
      thirdStageExecCnt.set(0);
      fourthStageExecCnt.set(0);
    }

    @Override
    public String getPipelineName() {
      return pipelineName;
    }

    @Override
    public Process create(String processId) {
      StageParameters stageParams =
          StageParameters.builder().immediateRetries(0).maximumRetries(0).build();

      return new ProcessBuilder(processId)
          .execute("STAGE0", stageParams)
          .with(
              (pipelineName1, processId1, stage1) -> {
                firstStageExecCnt.incrementAndGet();
                return firstStageExecResult;
              })
          .executeAfterPrevious("STAGE1", stageParams)
          .with(
              (pipelineName1, processId1, stage1) -> {
                secondStageExecCnt.incrementAndGet();
                return secondStageExecResult;
              })
          .executeAfterPrevious("STAGE2", stageParams)
          .with(
              (pipelineName1, processId1, stage1) -> {
                thirdStageExecCnt.incrementAndGet();
                return thirdStageExecResult;
              })
          .executeAfterPrevious("STAGE3", stageParams)
          .with(
              (pipelineName1, processId1, stage1) -> {
                fourthStageExecCnt.incrementAndGet();
                return fourthStageExecResult;
              })
          .build();
    }
  }

  private PipeliteLauncher pipeliteLauncher(String pipelineName) {
    launcherConfiguration.setPipelineName(pipelineName);
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    return pipeliteLauncher;
  }

  public void test(TestProcessFactory f, TestProcessSource s) {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncher(f.getPipelineName());
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);

    assertThat(s.getNewProcesses()).isEqualTo(0);
    assertThat(s.getReturnedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(s.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(s.getRejectedProcesses()).isEqualTo(0);

    assertLauncherStats(pipeliteLauncher, f);
    for (String processId : f.processIds) {
      assertProcessEntity(f, processId);
      assertStageEntities(f, processId);
    }
  }

  private void assertProcessEntity(TestProcessFactory f, String processId) {
    String pipelineName = f.getPipelineName();

    ProcessEntity processEntity =
        processService.getSavedProcess(f.getPipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    if (f.getFirstStageExecResult().isSuccess()
        && f.getSecondStageExecResult().isSuccess()
        && f.getThirdStageExecResult().isSuccess()
        && f.getFourthStageExecResult().isSuccess()) {
      assertThat(processEntity.getState()).isEqualTo(ProcessState.COMPLETED);
    } else {
      assertThat(processEntity.getState())
          .isEqualTo(ProcessState.FAILED); // no re-executions allowed
    }
  }

  private void assertStageEntities(TestProcessFactory f, String processId) {
    String pipelineName = f.getPipelineName();

    int stageCnt = 4;
    for (int i = 0; i < stageCnt; ++i) {
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

      if ((i == 0 && f.getFirstStageExecResult().isError())
          || (i == 1 && f.getSecondStageExecResult().isError())
          || (i == 2 && f.getThirdStageExecResult().isError())
          || (i == 3 && f.getFourthStageExecResult().isError())) {
        assertThat(stageEntity.getResultType()).isEqualTo(StageExecutionResultType.ERROR);
        assertThat(stageEntity.getResultParams()).isNull();
      } else {
        assertThat(stageEntity.getResultType()).isEqualTo(StageExecutionResultType.SUCCESS);
        assertThat(stageEntity.getResultParams()).isNull();
      }
    }
  }

  private void assertLauncherStats(PipeliteLauncher pipeliteLauncher, TestProcessFactory f) {
    if (f.getFirstStageExecResult().isSuccess()
        && f.getSecondStageExecResult().isSuccess()
        && f.getThirdStageExecResult().isSuccess()
        && f.getFourthStageExecResult().isSuccess()) {
      assertThat(pipeliteLauncher.getStats().getProcessExecutionCount(ProcessState.COMPLETED))
          .isEqualTo(PROCESS_CNT);
      assertThat(pipeliteLauncher.getStats().getProcessExecutionCount(ProcessState.FAILED))
          .isEqualTo(0);
    } else {
      assertThat(pipeliteLauncher.getStats().getProcessExecutionCount(ProcessState.COMPLETED))
          .isEqualTo(0);
      assertThat(pipeliteLauncher.getStats().getProcessExecutionCount(ProcessState.FAILED))
          .isEqualTo(PROCESS_CNT);
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
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(stageSuccessCount);
    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(stageFailedCount);
  }

  public void testFirstStageFails() {
    test(firstStageFails, firstStageFailsSource);
  }

  public void testSecondStageFails() {
    test(secondStageFails, secondStageFailsSource);
  }

  public void testThirdStageFails() {
    test(thirdStageFails, thirdStageFailsSource);
  }

  public void testFourthStageFails() {
    test(fourthStageFails, fourthStageFailsSource);
  }

  public void testNoStageFails() {
    test(noStageFails, noStageFailsSource);
  }
}
