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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.collector.ProcessRunnerMetrics;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithServices;
import pipelite.time.Time;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.service.force=true",
      "pipelite.service.name=ProcessRunnerPoolTest",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@Transactional
public class ProcessRunnerPoolTest {

  private static final int PROCESS_CNT = 100;
  private static final String PIPELINE_NAME = PipeliteTestIdCreator.pipelineName();

  @Autowired private PipeliteConfiguration pipeliteConfiguration;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics metrics;

  private ProcessRunnerFactory createProcessRunnerFactory(
      AtomicLong lockProcessCnt, AtomicLong unlockProcessCnt) {
    boolean lockProcess = true;
    return (pipelineName, process) ->
        new ProcessRunner(
            pipeliteConfiguration, pipeliteServices, metrics, pipelineName, process, lockProcess) {

          @Override
          protected void lockProcess() {
            lockProcessCnt.incrementAndGet();
          }

          @Override
          protected void unlockProcess() {
            unlockProcessCnt.incrementAndGet();
          }
        };
  }

  private ProcessRunnerPool createProcessRunnerPool(
      AtomicLong lockProcessCnt, AtomicLong unlockProcessCnt) {
    return new ProcessRunnerPool(
        pipeliteConfiguration,
        pipeliteServices,
        metrics,
        PipeliteTestIdCreator.processRunnerPoolName(),
        createProcessRunnerFactory(lockProcessCnt, unlockProcessCnt));
  }

  private Process createProcess(Function<StageExecutorRequest, StageExecutorResult> callback) {
    String processId = PipeliteTestIdCreator.processId();
    ExecutorParameters executorParams = new ExecutorParameters();
    executorParams.setMaximumRetries(0);
    Process process =
        new ProcessBuilder(processId)
            .execute("STAGE1")
            .withAsyncTestExecutor(callback, executorParams)
            .build();
    ProcessEntity processEntity =
        ProcessEntity.createExecution(PIPELINE_NAME, processId, ProcessEntity.DEFAULT_PRIORITY);
    process.setProcessEntity(processEntity);
    return process;
  }

  @Test
  public void testSuccess() {
    AtomicLong lockProcessCnt = new AtomicLong();
    AtomicLong unlockProcessCnt = new AtomicLong();
    ProcessRunnerPool pool = createProcessRunnerPool(lockProcessCnt, unlockProcessCnt);
    AtomicInteger runProcessCnt = new AtomicInteger();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process = createProcess((request) -> StageExecutorResult.success());
      pool.runProcess(PIPELINE_NAME, process, (p) -> runProcessCnt.incrementAndGet());
    }

    while (!pool.isIdle()) {
      Time.wait(Duration.ofSeconds(1));
      pool.runOneIteration();
    }

    ProcessRunnerMetrics processRunnerMetrics = metrics.process(PIPELINE_NAME);

    assertThat(runProcessCnt.get()).isEqualTo(PROCESS_CNT);

    assertThat(processRunnerMetrics.completedCount()).isEqualTo(PROCESS_CNT);
    assertThat(processRunnerMetrics.failedCount()).isZero();
    assertThat(metrics.error().count()).isZero();

    assertThat(lockProcessCnt.get()).isEqualTo(PROCESS_CNT);
    assertThat(unlockProcessCnt.get()).isEqualTo(PROCESS_CNT);
    assertThat(pool.getActiveProcessCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }

  @Test
  public void testFailed() {
    AtomicLong lockProcessCnt = new AtomicLong();
    AtomicLong unlockProcessCnt = new AtomicLong();
    ProcessRunnerPool pool = createProcessRunnerPool(lockProcessCnt, unlockProcessCnt);
    AtomicInteger runProcessCnt = new AtomicInteger();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process = createProcess((request) -> StageExecutorResult.executionError());
      pool.runProcess(PIPELINE_NAME, process, (p) -> runProcessCnt.incrementAndGet());
    }

    while (!pool.isIdle()) {
      Time.wait(Duration.ofSeconds(1));
      pool.runOneIteration();
    }
    ProcessRunnerMetrics processRunnerMetrics = metrics.process(PIPELINE_NAME);

    assertThat(runProcessCnt.get()).isEqualTo(PROCESS_CNT);

    assertThat(processRunnerMetrics.completedCount()).isZero();
    assertThat(processRunnerMetrics.failedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.error().count()).isZero();

    assertThat(lockProcessCnt.get()).isEqualTo(PROCESS_CNT);
    assertThat(unlockProcessCnt.get()).isEqualTo(PROCESS_CNT);
    assertThat(pool.getActiveProcessCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }
}
