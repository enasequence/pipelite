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
package pipelite.runner.process;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static pipelite.runner.process.ProcessQueue.MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.service.ProcessService;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=ProcessQueueTest",
      "pipelite.advanced.processQueueMinRefreshFrequency=0s",
      "pipelite.advanced.processQueueMaxRefreshFrequency=1d"
    })
@DirtiesContext
@ActiveProfiles("test")
@Transactional
public class ProcessQueueTest {

  @Autowired PipeliteConfiguration pipeliteConfiguration;
  @Autowired PipeliteServices pipeliteServices;
  @MockBean ProcessService processService;

  private static final int ACTIVE_PROCESS_CNT = 25;
  private static final int PENDING_PROCESS_CNT = 10;

  private class TestPipeline extends ConfigurableTestPipeline {
    public TestPipeline() {
      super(
          10,
          ACTIVE_PROCESS_CNT + PENDING_PROCESS_CNT,
          new TestProcessConfiguration() {
            @Override
            protected void configure(ProcessBuilder builder) {
              builder.execute("STAGE").withSyncTestExecutor().build();
            }
          });
    }
  }

  @Test
  public void isRefreshQueuePremature() {
    ZonedDateTime now = ZonedDateTime.now();
    assertThat(
            ProcessQueue.isRefreshQueuePremature(
                now, now.minus(Duration.ofMinutes(10)), Duration.ofMinutes(20)))
        .isTrue();
    assertThat(
            ProcessQueue.isRefreshQueuePremature(
                now, now.minus(Duration.ofMinutes(10)), Duration.ofMinutes(5)))
        .isFalse();
  }

  @Test
  public void isRefreshQueueOverdue() {
    ZonedDateTime now = ZonedDateTime.now();
    assertThat(
            ProcessQueue.isRefreshQueueOverdue(
                now, now.minus(Duration.ofMinutes(10)), Duration.ofMinutes(20)))
        .isFalse();
    assertThat(
            ProcessQueue.isRefreshQueueOverdue(
                now, now.minus(Duration.ofMinutes(10)), Duration.ofMinutes(5)))
        .isTrue();
  }

  @Test
  public void lifecycle() {
    final TestPipeline pipeline = new TestPipeline();
    final int pipelineParallelism = pipeline.configurePipeline().pipelineParallelism();
    final int processQueueMaxSize = pipelineParallelism * MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER;

    List<ProcessEntity> activeEntities =
        Collections.nCopies(ACTIVE_PROCESS_CNT, mock(ProcessEntity.class));
    List<ProcessEntity> pendingEntities =
        Collections.nCopies(PENDING_PROCESS_CNT, mock(ProcessEntity.class));
    doReturn(activeEntities)
        .when(processService)
        .getUnlockedActiveProcesses(any(), eq(processQueueMaxSize));
    doReturn(pendingEntities)
        .when(processService)
        .getPendingProcesses(any(), eq(processQueueMaxSize - ACTIVE_PROCESS_CNT));

    ProcessQueue processQueue =
        spy(new ProcessQueue(pipeliteConfiguration, pipeliteServices, pipeline));

    assertThat(processQueue.isRefreshQueue()).isTrue();
    assertThat(processQueue.getProcessQueueSize()).isZero();
    assertThat(processQueue.getProcessQueueMaxSize()).isEqualTo(processQueueMaxSize);
    assertThat(processQueue.getProcessQueueRefreshSize()).isZero();

    // Queue processes.
    processQueue.refreshQueue();
    verify(processQueue, times(1)).refreshQueue();

    assertThat(processQueue.isRefreshQueue()).isFalse();
    assertThat(processQueue.getProcessQueueSize())
        .isEqualTo(ACTIVE_PROCESS_CNT + PENDING_PROCESS_CNT);
    assertThat(processQueue.getProcessQueueMaxSize()).isEqualTo(processQueueMaxSize);
    assertThat(processQueue.getProcessQueueRefreshSize())
        .isEqualTo(ACTIVE_PROCESS_CNT + PENDING_PROCESS_CNT);

    for (int i = 0; i < ACTIVE_PROCESS_CNT + PENDING_PROCESS_CNT; ++i) {
      assertThat(processQueue.nextProcess(0)).isNotNull();
    }
    assertThat(processQueue.isRefreshQueue()).isTrue();
    assertThat(processQueue.getProcessQueueSize()).isZero();
    assertThat(processQueue.getProcessQueueMaxSize()).isEqualTo(processQueueMaxSize);
    assertThat(processQueue.getProcessQueueRefreshSize())
        .isEqualTo(ACTIVE_PROCESS_CNT + PENDING_PROCESS_CNT);
  }
}
