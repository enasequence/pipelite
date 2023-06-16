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
package pipelite.runner.process;

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
import org.springframework.transaction.annotation.Transactional;
import pipelite.Pipeline;
import pipelite.entity.ProcessEntity;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.collector.ProcessRunnerMetrics;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorState;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithManager;

/**
 * If process configuration fails for a pipeline then the process will not be executed. No stages
 * will be created for the process.
 */
@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.service.force=true",
      "pipelite.service.name=ProcessRunnerPoolFailedToCreateProcessTest",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"pipelite", "ProcessRunnerPoolFailedToCreateProcessTest"})
@DirtiesContext
@Transactional
public class ProcessRunnerPoolFailedToCreateProcessTest {

  private static final String PIPELINE_NAME = PipeliteTestIdCreator.pipelineName();
  private static final String PROCESS_ID = PipeliteTestIdCreator.processId();

  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private PipeliteMetrics pipeliteMetrics;

  @Profile("ProcessRunnerPoolFailedToCreateProcessTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public MissingStageTestPipeline pipeline() {
      return new MissingStageTestPipeline();
    }
  }

  static class MissingStageTestPipeline implements Pipeline {

    @Override
    public Options configurePipeline() {
      return new Options().pipelineParallelism(1);
    }

    @Override
    public String pipelineName() {
      return PIPELINE_NAME;
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {
      builder
          .executeAfter("STAGE", "MISSING_STAGE")
          .withSyncTestExecutor(StageExecutorState.SUCCESS);
    }

    private int nextProcessCount = 0;

    @Override
    public Process nextProcess() {
      return nextProcessCount++ == 0 ? new Process(PROCESS_ID) : null;
    }
  }

  @Test
  public void testProcess() {
    List<ProcessRunnerPool> pools = processRunnerPoolManager.createPools();
    assertThat(pools.size()).isOne();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();
    assertThat(pools.get(0).getActiveProcessCount()).isZero();
    assertThat(pools.get(0).getActiveProcessRunners().size()).isZero();

    ProcessEntity processEntity =
        pipeliteServices.process().getSavedProcess(PIPELINE_NAME, PROCESS_ID).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(PIPELINE_NAME);
    assertThat(processEntity.getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.PENDING);

    ProcessRunnerMetrics processRunnerMetrics = pipeliteMetrics.process(PIPELINE_NAME);
    assertThat(processRunnerMetrics.completedCount()).isZero();
    assertThat(processRunnerMetrics.failedCount()).isZero();
    assertThat(pipeliteMetrics.error().count()).isOne();
  }
}
