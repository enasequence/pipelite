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

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Value;
import org.springframework.beans.factory.ObjectProvider;
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
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.executor.StageExecutorParameters;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

@Component
@Scope("prototype")
public class PipeliteLauncherTester {

  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;
  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private TestProcessFactory testProcessFactory;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;

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
      return new TestProcessFactory("processSuccess", 10, 2, StageTestResult.SUCCESS);
    }

    @Bean("processFailure")
    public TestProcessFactory processFailure() {
      return new TestProcessFactory("processFailure", 10, 2, StageTestResult.ERROR);
    }

    @Bean("processException")
    public TestProcessFactory processException() {
      return new TestProcessFactory("processException", 10, 2, StageTestResult.EXCEPTION);
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
                    return StageExecutionResult.error();
                  }
                  if (stageTestResult == StageTestResult.SUCCESS) {
                    return StageExecutionResult.success();
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
    PipeliteLauncherStats stats = pipeliteLauncher.getStats();

    assertThat(stats.getProcessCreationFailedCount()).isEqualTo(0);
    assertThat(stats.getProcessExceptionCount()).isEqualTo(0);

    if (f.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(stats.getProcessExecutionCount(ProcessState.FAILED))
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(stats.getStageFailedCount()).isEqualTo(f.stageExecCnt.get());
      assertThat(stats.getStageSuccessCount()).isEqualTo(0L);

    } else {
      assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED))
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(stats.getStageFailedCount()).isEqualTo(0L);
      assertThat(stats.getStageSuccessCount()).isEqualTo(f.stageExecCnt.get());
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
        assertThat(stageEntity.getResultType()).isEqualTo(StageExecutionResultType.ERROR);
        assertThat(stageEntity.getResultParams()).isNull();
      } else if (f.stageTestResult == StageTestResult.EXCEPTION) {
        assertThat(stageEntity.getResultType()).isEqualTo(StageExecutionResultType.ERROR);
        assertThat(stageEntity.getResultParams())
            .contains("exception\" : \"java.lang.RuntimeException: Expected exception");
      } else {
        assertThat(stageEntity.getResultType()).isEqualTo(StageExecutionResultType.SUCCESS);
        assertThat(stageEntity.getResultParams()).isNull();
      }
    }
  }

  private void test(TestProcessFactory f) {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    pipeliteLauncher.setPipelineName(f.getPipelineName());
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);

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
