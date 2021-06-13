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
import pipelite.helper.*;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;
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
  private static final int IMMEDIATE_RETRIES = 3;
  private static final int MAXIMUM_RETRIES = 3;
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

  protected static class TestPipeline extends SingleStageSimpleLsfTestProcessFactory {
    public TestPipeline(int exitCode, LsfTestConfiguration lsfTestConfiguration) {
      super(
          PROCESS_CNT,
          PARALLELISM,
          exitCode,
          IMMEDIATE_RETRIES,
          MAXIMUM_RETRIES,
          lsfTestConfiguration);
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

  private TestType getTestType(TestPipeline testPipeline) {
    if (testPipeline instanceof PermanentErrorPipeline) {
      return TestType.PERMANENT_ERROR;
    }
    if (testPipeline instanceof NonPermanentErrorPipeline) {
      return TestType.NON_PERMANENT_ERROR;
    }
    return TestType.SUCCESS;
  }

  private void assertMetrics(TestPipeline f) {
    MetricsTestHelper.assertCompletedMetrics(
        getTestType(f), metrics, f.pipelineName(), PROCESS_CNT, IMMEDIATE_RETRIES, MAXIMUM_RETRIES);
  }

  private void assertProcessEntity(TestPipeline f, String processId) {
    ProcessEntityTestHelper.assertProcessEntity(
        processService, f.pipelineName(), processId, getTestType(f));
  }

  private void assertStageEntity(TestPipeline f, String processId) {
    StageEntityTestHelper.assertCompletedSimpleLsfExecutorStageEntity(
        getTestType(f),
        stageService,
        f.pipelineName(),
        processId,
        f.stageName(),
        f.executorParams().getPermanentErrors(),
        f.cmd(),
        f.exitCode(),
        IMMEDIATE_RETRIES,
        MAXIMUM_RETRIES);
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
  public void runPipelines() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    for (TestPipeline f : testPipelines) {
      assertPipeline(f);
    }
  }
}
