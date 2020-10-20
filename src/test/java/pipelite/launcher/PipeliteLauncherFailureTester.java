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

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import pipelite.TestInMemoryProcessFactory;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.ErrorStageExecutor;
import pipelite.executor.SuccessStageExecutor;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.builder.ProcessBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Component
public class PipeliteLauncherFailureTester {

  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory firstStageFails() {
      return new TestInMemoryProcessFactory(FIRST_STAGE_FAILS_NAME, FIRST_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessFactory secondStageFails() {
      return new TestInMemoryProcessFactory(SECOND_STAGE_FAILS_NAME, SECOND_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessFactory thirdStageFails() {
      return new TestInMemoryProcessFactory(THIRD_STAGE_FAILS_NAME, THIRD_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessFactory fourthStageFails() {
      return new TestInMemoryProcessFactory(FOURTH_STAGE_FAILS_NAME, FOURTH_STAGE_FAILS_PROCESSES);
    }

    @Bean
    public ProcessFactory noStageFails() {
      return new TestInMemoryProcessFactory(NO_STAGE_FAILS_NAME, NO_STAGE_FAILS_PROCESSES);
    }
  }

  private static final String FIRST_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String SECOND_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String THIRD_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String FOURTH_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String NO_STAGE_FAILS_NAME = UniqueStringGenerator.randomPipelineName();

  private static final int PROCESS_CNT = 5;
  private static final List<Process> FIRST_STAGE_FAILS_PROCESSES = new ArrayList<>();
  private static final List<Process> SECOND_STAGE_FAILS_PROCESSES = new ArrayList<>();
  private static final List<Process> THIRD_STAGE_FAILS_PROCESSES = new ArrayList<>();
  private static final List<Process> FOURTH_STAGE_FAILS_PROCESSES = new ArrayList<>();
  private static final List<Process> NO_STAGE_FAILS_PROCESSES = new ArrayList<>();

  static {
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              FIRST_STAGE_FAILS_PROCESSES.add(
                  new ProcessBuilder(
                          FIRST_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .execute("STAGE1")
                      .with(new ErrorStageExecutor())
                      .executeAfterPrevious("STAGE2")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE3")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE4")
                      .with(new SuccessStageExecutor())
                      .build());
              SECOND_STAGE_FAILS_PROCESSES.add(
                  new ProcessBuilder(
                          SECOND_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .execute("STAGE1")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE2")
                      .with(new ErrorStageExecutor())
                      .executeAfterPrevious("STAGE3")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE4")
                      .with(new SuccessStageExecutor())
                      .build());
              THIRD_STAGE_FAILS_PROCESSES.add(
                  new ProcessBuilder(
                          THIRD_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .execute("STAGE1")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE2")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE3")
                      .with(new ErrorStageExecutor())
                      .executeAfterPrevious("STAGE4")
                      .with(new SuccessStageExecutor())
                      .build());
              FOURTH_STAGE_FAILS_PROCESSES.add(
                  new ProcessBuilder(
                          FOURTH_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .execute("STAGE1")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE2")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE3")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE4")
                      .with(new ErrorStageExecutor())
                      .build());
              NO_STAGE_FAILS_PROCESSES.add(
                  new ProcessBuilder(
                          NO_STAGE_FAILS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .execute("STAGE1")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE2")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE3")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE4")
                      .with(new SuccessStageExecutor())
                      .build());
            });
  }

  private PipeliteLauncher init(String pipelineName, ProcessSource processSource) {
    processConfiguration.setPipelineName(pipelineName);
    processConfiguration.setProcessSource(processSource);
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    pipeliteLauncher.setShutdownIfIdle(true);
    return pipeliteLauncher;
  }

  public void testFirstStageFails() {
    TestInMemoryProcessSource processSource =
        new TestInMemoryProcessSource(FIRST_STAGE_FAILS_PROCESSES);
    PipeliteLauncher pipeliteLauncher = init(FIRST_STAGE_FAILS_NAME, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
  }

  public void testSecondStageFails() {
    TestInMemoryProcessSource processSource =
        new TestInMemoryProcessSource(SECOND_STAGE_FAILS_PROCESSES);
    PipeliteLauncher pipeliteLauncher = init(SECOND_STAGE_FAILS_NAME, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
  }

  public void testThirdStageFails() {
    TestInMemoryProcessSource processSource =
        new TestInMemoryProcessSource(THIRD_STAGE_FAILS_PROCESSES);
    PipeliteLauncher pipeliteLauncher = init(THIRD_STAGE_FAILS_NAME, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT * 2);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
  }

  public void testFourthStageFails() {
    TestInMemoryProcessSource processSource =
        new TestInMemoryProcessSource(FOURTH_STAGE_FAILS_PROCESSES);
    PipeliteLauncher pipeliteLauncher = init(FOURTH_STAGE_FAILS_NAME, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT * 3);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
  }

  public void testNoStageFails() {
    TestInMemoryProcessSource processSource =
        new TestInMemoryProcessSource(NO_STAGE_FAILS_PROCESSES);
    PipeliteLauncher pipeliteLauncher = init(NO_STAGE_FAILS_NAME, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcesses()).isEqualTo(0);
    assertThat(processSource.getReturnedProcesses()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcesses()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcesses()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT * 4);
    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(0);
  }
}
