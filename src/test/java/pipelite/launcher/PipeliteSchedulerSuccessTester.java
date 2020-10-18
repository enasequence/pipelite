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

import lombok.AllArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ScheduleEntity;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.builder.ProcessBuilder;
import pipelite.repository.ScheduleRepository;
import pipelite.stage.StageExecutionResult;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class PipeliteSchedulerSuccessTester {

  private final ObjectProvider<PipeliteScheduler> pipeliteSchedulerObjectProvider;
  private final ScheduleRepository scheduleRepository;
  private final LauncherConfiguration launcherConfiguration;

  private static final Duration STOP_AFTER = Duration.ofSeconds(10);
  private static final int SCHEDULE_SECONDS = 2; // 60 must be divisible by SCHEDULE_SECONDS.

  private static class ExecutionCounter {
    public final AtomicInteger processExecutionCount = new AtomicInteger();
    public final AtomicInteger stageExecutionCount = new AtomicInteger();

    public void reset() {
      processExecutionCount.set(0);
      stageExecutionCount.set(0);
    }
  }

  public static class TestProcessFactory implements ProcessFactory {
    public final String pipelineName;
    public final ExecutionCounter executionCounter;

    public TestProcessFactory(String pipelineName, ExecutionCounter executionCounter) {
      this.pipelineName = pipelineName;
      this.executionCounter = executionCounter;
    }

    @Override
    public String getPipelineName() {
      return pipelineName;
    }

    @Override
    public Process create(String processId) {
      return new ProcessBuilder(pipelineName, processId, 9)
          .execute("STAGE1")
          .with(
              (stage) -> {
                //System.out.println(
                //    "executing process: " + processId + " stage: " + stage.getStageName());
                executionCounter.processExecutionCount.incrementAndGet();
                executionCounter.stageExecutionCount.incrementAndGet();
                return StageExecutionResult.success();
              })
          .execute("STAGE2")
          .with(
              (stage) -> {
                //System.out.println(
                //    "executing process: " + processId + " stage: " + stage.getStageName());
                executionCounter.stageExecutionCount.incrementAndGet();
                return StageExecutionResult.success();
              })
          .build();
    }
  }

  private static final String PIPELINE_NAME_1 = UniqueStringGenerator.randomPipelineName();
  private static final ExecutionCounter EXECUTION_COUNTER_1 = new ExecutionCounter();
  public static class TestProcessFactory1 extends TestProcessFactory {
    public TestProcessFactory1() {
      super(PIPELINE_NAME_1, EXECUTION_COUNTER_1);
    }
  }

  private void saveSchedule1() {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setSchedule("0/" + SCHEDULE_SECONDS + " * * * * ?");
    schedule.setLauncherName(launcherConfiguration.getLauncherName());
    schedule.setPipelineName(PIPELINE_NAME_1);
    schedule.setProcessFactoryName(TestProcessFactory1.class.getName());
    scheduleRepository.save(schedule);
  }

  public void testSingleProcess() {
    EXECUTION_COUNTER_1.reset();

    saveSchedule1();

    PipeliteScheduler pipeliteScheduler = pipeliteSchedulerObjectProvider.getObject();
    pipeliteScheduler.setShutdownAfter(STOP_AFTER);

    ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

    long expectedProcessCompletedCount = STOP_AFTER.toMillis() / 1000 / SCHEDULE_SECONDS;
    long expectedStageCompletedCount = expectedProcessCompletedCount * 2;

    assertThat(EXECUTION_COUNTER_1.processExecutionCount.get()).isEqualTo(expectedProcessCompletedCount);
    assertThat(EXECUTION_COUNTER_1.stageExecutionCount.get()).isEqualTo(expectedStageCompletedCount);
    assertThat(pipeliteScheduler.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getProcessFailedToCreateCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getProcessFailedToExecuteCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getProcessCompletedCount())
        .isEqualTo(expectedProcessCompletedCount);
    assertThat(pipeliteScheduler.getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getStageCompletedCount()).isEqualTo(expectedStageCompletedCount);
  }
}
