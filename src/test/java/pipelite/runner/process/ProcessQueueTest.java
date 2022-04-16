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
import static pipelite.runner.process.ProcessQueue.MIN_QUEUE_SIZE_INCREASE;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
import pipelite.runner.process.creator.ProcessEntityCreator;
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
  private static final int PARALLELISM = 10;
  private static final int FIRST_REFRESH_CREATE_CNT =
      ProcessQueue.defaultMaxQueueSize(PARALLELISM) - ACTIVE_PROCESS_CNT - PENDING_PROCESS_CNT;
  private static final int SECOND_REFRESH_CREATE_CNT =
      PARALLELISM * MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER;
  private static final int THIRD_REFRESH_CREATE_CNT =
      PARALLELISM * MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER;

  // Refresh will be called twice.
  private class TestPipeline extends ConfigurableTestPipeline {
    public TestPipeline() {
      super(
          PARALLELISM,
          FIRST_REFRESH_CREATE_CNT + SECOND_REFRESH_CREATE_CNT + THIRD_REFRESH_CREATE_CNT,
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
    final int processQueueMaxSize1 = ProcessQueue.defaultMaxQueueSize(PARALLELISM);
    final int processQueueMinSize1 = ProcessQueue.defaultMinQueueSize(PARALLELISM);
    final int processQueueMaxSize2 = PARALLELISM * MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER;
    final int processQueueMinSize2 = 8;
    final int processQueueMaxSize3 = PARALLELISM * MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER;
    final int processQueueMinSize3 = 12;

    List<ProcessEntity> activeEntities =
        Collections.nCopies(ACTIVE_PROCESS_CNT, mock(ProcessEntity.class));
    List<ProcessEntity> pendingEntities =
        Collections.nCopies(PENDING_PROCESS_CNT, mock(ProcessEntity.class));

    doReturn(activeEntities)
        .when(processService)
        .getUnlockedActiveProcesses(any(), eq(processQueueMaxSize1));
    doReturn(pendingEntities)
        .when(processService)
        .getPendingProcesses(any(), eq(processQueueMaxSize1 - ACTIVE_PROCESS_CNT));

    doAnswer(
            invocation -> {
              String pipelineName = invocation.getArgument(0);
              String processId = invocation.getArgument(1);
              ProcessEntity processEntity = new ProcessEntity();
              processEntity.setPipelineName(pipelineName);
              processEntity.setProcessId(processId);
              return Optional.of(processEntity);
            })
        .when(processService)
        .getSavedProcess(any(), any());

    ProcessEntityCreator processEntityCreator = new ProcessEntityCreator(pipeline, processService);

    ProcessQueue processQueue =
        spy(
            new ProcessQueue(
                pipeliteConfiguration, pipeliteServices, processEntityCreator, pipeline));

    assertThat(processQueue.isRefreshQueue()).isTrue();
    assertThat(processQueue.getCurrentQueueSize()).isZero();
    assertThat(processQueue.getProcessQueueSize().max()).isEqualTo(processQueueMaxSize1);
    assertThat(processQueue.getProcessQueueSize().min()).isEqualTo(processQueueMinSize1);
    assertThat(processQueue.getRefreshQueueSize()).isZero();

    // Refresh queue.
    processQueue.refreshQueue();
    verify(processQueue, times(1)).refreshQueue();
    verify(processQueue, times(0)).adjustQueue();

    assertThat(processQueue.isRefreshQueue()).isFalse();
    assertThat(processQueue.getCurrentQueueSize()).isEqualTo(processQueueMaxSize1);
    assertThat(processQueue.getProcessQueueSize().max()).isEqualTo(processQueueMaxSize1);
    assertThat(processQueue.getProcessQueueSize().min()).isEqualTo(processQueueMinSize1);
    assertThat(processQueue.getRefreshQueueSize()).isEqualTo(processQueueMaxSize1);
    assertThat(processQueue.getActiveProcessCnt()).isEqualTo(ACTIVE_PROCESS_CNT);
    assertThat(processQueue.getPendingProcessCnt()).isEqualTo(PENDING_PROCESS_CNT);
    assertThat(processQueue.getCreatedProcessCnt())
        .isEqualTo(processQueueMaxSize1 - ACTIVE_PROCESS_CNT - PENDING_PROCESS_CNT);

    for (int i = 0; i < processQueueMaxSize1; ++i) {
      assertThat(processQueue.nextProcess(0)).isNotNull();
    }
    assertThat(processQueue.isRefreshQueue()).isTrue();
    assertThat(processQueue.getCurrentQueueSize()).isZero();
    assertThat(processQueue.getProcessQueueSize().max()).isEqualTo(processQueueMaxSize1);
    assertThat(processQueue.getProcessQueueSize().min()).isEqualTo(processQueueMinSize1);
    assertThat(processQueue.getRefreshQueueSize()).isEqualTo(processQueueMaxSize1);

    // Refresh queue.
    processQueue.refreshQueue();
    verify(processQueue, times(2)).refreshQueue();
    verify(processQueue, times(1)).adjustQueue();

    assertThat(processQueue.isRefreshQueue()).isFalse();
    assertThat(processQueue.getCurrentQueueSize()).isEqualTo(processQueueMaxSize2);
    assertThat(processQueue.getProcessQueueSize().max()).isEqualTo(processQueueMaxSize2);
    assertThat(processQueue.getProcessQueueSize().min()).isEqualTo(processQueueMinSize2);
    assertThat(processQueue.getRefreshQueueSize()).isEqualTo(processQueueMaxSize2);
    assertThat(processQueue.getActiveProcessCnt()).isEqualTo(0);
    assertThat(processQueue.getPendingProcessCnt()).isEqualTo(0);
    assertThat(processQueue.getCreatedProcessCnt()).isEqualTo(processQueueMaxSize2);

    for (int i = 0; i < processQueueMaxSize2; ++i) {
      assertThat(processQueue.nextProcess(0)).isNotNull();
    }
    assertThat(processQueue.isRefreshQueue()).isTrue();
    assertThat(processQueue.getCurrentQueueSize()).isZero();

    // Refresh queue.
    processQueue.refreshQueue();
    verify(processQueue, times(3)).refreshQueue();
    verify(processQueue, times(2)).adjustQueue();

    assertThat(processQueue.isRefreshQueue()).isFalse();
    assertThat(processQueue.getCurrentQueueSize()).isEqualTo(processQueueMaxSize3);
    assertThat(processQueue.getProcessQueueSize().max()).isEqualTo(processQueueMaxSize3);
    assertThat(processQueue.getProcessQueueSize().min()).isEqualTo(processQueueMinSize3);
    assertThat(processQueue.getRefreshQueueSize()).isEqualTo(processQueueMaxSize3);
    assertThat(processQueue.getActiveProcessCnt()).isEqualTo(0);
    assertThat(processQueue.getPendingProcessCnt()).isEqualTo(0);
    assertThat(processQueue.getCreatedProcessCnt()).isEqualTo(processQueueMaxSize3);
  }

  @Test
  public void isRefreshQueue() {
    // true: no refresh time
    assertThat(
            ProcessQueue.isRefreshQueue(
                null, // refreshTime
                Duration.ofMinutes(1), // minRefreshFrequency
                Duration.ofMinutes(10), // maxRefreshFrequency
                new ProcessQueue.ProcessQueueSize(10, 40),
                0))
        .isTrue();
    // false: isRefreshQueuePremature
    assertThat(
            ProcessQueue.isRefreshQueue(
                ZonedDateTime.now().minus(Duration.ofSeconds(1)), // refreshTime
                Duration.ofMinutes(1), // minRefreshFrequency
                Duration.ofMinutes(10), // maxRefreshFrequency
                new ProcessQueue.ProcessQueueSize(10, 40),
                0))
        .isFalse();
    // true: isRefreshQueueOverdue
    assertThat(
            ProcessQueue.isRefreshQueue(
                ZonedDateTime.now().minus(Duration.ofHours(1)), // refreshTime
                Duration.ofMinutes(1), // minRefreshFrequency
                Duration.ofMinutes(10), // maxRefreshFrequency
                new ProcessQueue.ProcessQueueSize(10, 40),
                0))
        .isTrue();
    // true: currentQueueSize < processQueueSize.min()
    assertThat(
            ProcessQueue.isRefreshQueue(
                ZonedDateTime.now().minus(Duration.ofMinutes(1)), // refreshTime
                Duration.ofMinutes(1), // minRefreshFrequency
                Duration.ofMinutes(10), // maxRefreshFrequency
                new ProcessQueue.ProcessQueueSize(10, 40),
                1))
        .isTrue();
    // false: currentQueueSize >= processQueueSize.min()
    assertThat(
            ProcessQueue.isRefreshQueue(
                ZonedDateTime.now().minus(Duration.ofMinutes(1)), // refreshTime
                Duration.ofMinutes(1), // minRefreshFrequency
                Duration.ofMinutes(10), // maxRefreshFrequency
                new ProcessQueue.ProcessQueueSize(10, 40),
                10))
        .isFalse();
  }

  @Test
  public void adjustQueueTooFrequentNotFilledNotEmpty_NoAdjustment() {
    // Current refresh frequency < maximum refresh frequency.
    // Queue was not filled during last refresh.
    // Queue was not empty when it was refreshed.

    int minSize = 10;
    int maxSize = 40;
    ProcessQueue.ProcessQueueSize defaultProcessQueueSize =
        new ProcessQueue.ProcessQueueSize(minSize, maxSize);

    ProcessQueue.ProcessQueueSize processQueueSize =
        ProcessQueue.adjustQueue(
            "TEST_PIPELINE",
            10,
            defaultProcessQueueSize,
            maxSize - 1, // refreshQueueSize
            minSize, // currentQueueSize
            Duration.ofMinutes(10), // maxRefreshFrequency
            Duration.ofMinutes(1)); // currentRefreshFrequency
    assertThat(processQueueSize).isEqualTo(defaultProcessQueueSize);

    processQueueSize =
        ProcessQueue.adjustQueue(
            "TEST_PIPELINE",
            10,
            defaultProcessQueueSize,
            minSize, // refreshQueueSize
            minSize, // currentQueueSize
            Duration.ofMinutes(10), // maxRefreshFrequency
            Duration.ofMinutes(1)); // currentRefreshFrequency
    assertThat(processQueueSize).isEqualTo(defaultProcessQueueSize);

    processQueueSize =
        ProcessQueue.adjustQueue(
            "TEST_PIPELINE",
            10,
            defaultProcessQueueSize,
            0, // refreshQueueSize
            0, // currentQueueSize
            Duration.ofMinutes(10), // maxRefreshFrequency
            Duration.ofMinutes(1)); // currentRefreshFrequency
    assertThat(processQueueSize).isEqualTo(defaultProcessQueueSize);
  }

  @Test
  public void adjustQueueTooFrequentFilledNotEmpty_MaxAdjustment() {
    // Current refresh frequency < maximum refresh frequency.
    // Queue was filled during last refresh.
    // Queue was not empty when it was refreshed.

    int minSize = 10;
    int maxSize = 40;
    int tooFrequentMultiplier1 = 5;
    int tooFrequentMultiplier2 = 3;
    ProcessQueue.ProcessQueueSize defaultProcessQueueSize =
        new ProcessQueue.ProcessQueueSize(minSize, maxSize);

    ProcessQueue.ProcessQueueSize processQueueSize =
        ProcessQueue.adjustQueue(
            "TEST_PIPELINE",
            10,
            defaultProcessQueueSize,
            maxSize, // refreshQueueSize
            minSize, // currentQueueSize
            Duration.ofMinutes(17).multipliedBy(tooFrequentMultiplier1), // maxRefreshFrequency
            Duration.ofMinutes(17)); // currentRefreshFrequency
    assertThat(processQueueSize)
        .isEqualTo(new ProcessQueue.ProcessQueueSize(minSize, maxSize * tooFrequentMultiplier1));

    processQueueSize =
        ProcessQueue.adjustQueue(
            "TEST_PIPELINE",
            10,
            defaultProcessQueueSize,
            maxSize, // refreshQueueSize
            minSize, // currentQueueSize
            Duration.ofHours(5).multipliedBy(tooFrequentMultiplier2), // maxRefreshFrequency
            Duration.ofHours(5)); // currentRefreshFrequency
    assertThat(processQueueSize)
        .isEqualTo(new ProcessQueue.ProcessQueueSize(minSize, maxSize * tooFrequentMultiplier2));
  }

  @Test
  public void adjustQueueTooFrequentNotFilledEmpty_NoAdjustment() {
    // Current refresh frequency < refresh frequency.
    // Queue was not filled during last refresh.
    // Queue was empty when it was refreshed.

    int minSize = 10;
    int maxSize = 40;
    ProcessQueue.ProcessQueueSize defaultProcessQueueSize =
        new ProcessQueue.ProcessQueueSize(minSize, maxSize);

    ProcessQueue.ProcessQueueSize processQueueSize =
        ProcessQueue.adjustQueue(
            "TEST_PIPELINE",
            10,
            defaultProcessQueueSize,
            minSize, // refreshQueueSize
            0, // currentQueueSize
            Duration.ofMinutes(10), // maxRefreshFrequency
            Duration.ofMinutes(1)); // currentRefreshFrequency
    assertThat(processQueueSize).isEqualTo(defaultProcessQueueSize);
  }

  @Test
  public void adjustQueueNotTooFrequentFilledEmpty_MinAdjustment() {
    // Current refresh frequency >= refresh frequency.
    // Queue was filled during last refresh.
    // Queue was empty when it was refreshed.

    int minSize = 10;
    int maxSize = 40;
    ProcessQueue.ProcessQueueSize defaultProcessQueueSize =
        new ProcessQueue.ProcessQueueSize(minSize, maxSize);

    ProcessQueue.ProcessQueueSize processQueueSize =
        ProcessQueue.adjustQueue(
            "TEST_PIPELINE",
            10,
            defaultProcessQueueSize,
            maxSize, // refreshQueueSize
            0, // currentQueueSize
            Duration.ofMinutes(10), // maxRefreshFrequency
            Duration.ofMinutes(10)); // currentRefreshFrequency
    assertThat(processQueueSize)
        .isEqualTo(
            new ProcessQueue.ProcessQueueSize(
                (int) Math.ceil(minSize * MIN_QUEUE_SIZE_INCREASE), maxSize));
    assertThat(processQueueSize).isEqualTo(new ProcessQueue.ProcessQueueSize(15, maxSize));
  }
}
