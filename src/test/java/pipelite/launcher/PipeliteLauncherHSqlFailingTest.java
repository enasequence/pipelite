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
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import pipelite.TestConfiguration;
import pipelite.TestInMemoryProcessFactory;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.ErrorStageExecutor;
import pipelite.executor.SuccessStageExecutor;
import pipelite.process.Process;
import pipelite.process.ProcessSource;
import pipelite.process.builder.ProcessBuilder;

@SpringBootTest(
    classes = TestConfiguration.class,
    properties = {
      "pipelite.launcher.workers=5",
      "pipelite.launcher.processLaunchFrequency=250ms",
      "pipelite.launcher.stageLaunchFrequency=250ms",
      "pipelite.stage.retries=1"
    })
@ContextConfiguration(initializers = PipeliteLauncherHSqlSuccessTest.TestContextInitializer.class)
@ActiveProfiles(value = {"hsql-test"})
public class PipeliteLauncherHSqlFailingTest {

  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  private static final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private static final int PROCESS_CNT = 5;

  private PipeliteLauncher init(List<Process> processes, ProcessSource processSource) {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processes);
    processConfiguration.setProcessFactory(processFactory);
    processConfiguration.setProcessSource(processSource);
    pipeliteLauncher.setShutdownIfIdle(true);
    return pipeliteLauncher;
  }

  @Test
  public void testFirstStageFails() {
    processConfiguration.setPipelineName(PIPELINE_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PIPELINE_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .execute("STAGE1")
                      .with(new ErrorStageExecutor())
                      .executeAfterPrevious("STAGE2")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE3")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE4")
                      .with(new SuccessStageExecutor())
                      .build());
            });

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processes);
    PipeliteLauncher pipeliteLauncher = init(processes, processSource);
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

  @Test
  public void testSecondStageFails() {
    processConfiguration.setPipelineName(PIPELINE_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PIPELINE_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .execute("STAGE1")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE2")
                      .with(new ErrorStageExecutor())
                      .executeAfterPrevious("STAGE3")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE4")
                      .with(new SuccessStageExecutor())
                      .build());
            });

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processes);
    PipeliteLauncher pipeliteLauncher = init(processes, processSource);
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

  @Test
  public void testThirdStageFails() {
    processConfiguration.setPipelineName(PIPELINE_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PIPELINE_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .execute("STAGE1")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE2")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE3")
                      .with(new ErrorStageExecutor())
                      .executeAfterPrevious("STAGE4")
                      .with(new SuccessStageExecutor())
                      .build());
            });

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processes);
    PipeliteLauncher pipeliteLauncher = init(processes, processSource);
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

  @Test
  public void testFourthStageFails() {
    processConfiguration.setPipelineName(PIPELINE_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PIPELINE_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .execute("STAGE1")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE2")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE3")
                      .with(new SuccessStageExecutor())
                      .executeAfterPrevious("STAGE4")
                      .with(new ErrorStageExecutor())
                      .build());
            });

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processes);
    PipeliteLauncher pipeliteLauncher = init(processes, processSource);
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

  @Test
  public void testNoStageFails() {
    processConfiguration.setPipelineName(PIPELINE_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PIPELINE_NAME, UniqueStringGenerator.randomProcessId(), 9)
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

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processes);
    PipeliteLauncher pipeliteLauncher = init(processes, processSource);
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
