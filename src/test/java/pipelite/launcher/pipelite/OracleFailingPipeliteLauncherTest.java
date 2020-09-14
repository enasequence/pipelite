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
package pipelite.launcher.pipelite;

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
import pipelite.FullTestConfiguration;
import pipelite.TestInMemoryProcessFactory;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.ErrorTaskExecutor;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.launcher.ServerManager;
import pipelite.process.Process;
import pipelite.process.ProcessSource;
import pipelite.process.builder.ProcessBuilder;

@SpringBootTest(
    classes = FullTestConfiguration.class,
    properties = {
      "pipelite.launcher.workers=5",
      "pipelite.launcher.processLaunchFrequency=250ms",
      "pipelite.launcher.taskLaunchFrequency=250ms",
      "pipelite.task.retries=1"
    })
@ContextConfiguration(initializers = OracleSuccessPipeliteLauncherTest.TestContextInitializer.class)
@ActiveProfiles(value = {"oracle-test"})
public class OracleFailingPipeliteLauncherTest {

  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();
  private static final int PROCESS_CNT = 5;

  private PipeliteLauncher init(List<Process> processes, ProcessSource processSource) {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    processConfiguration.setProcessFactory(new TestInMemoryProcessFactory(processes));
    processConfiguration.setProcessSource(processSource);
    pipeliteLauncher.setShutdownIfIdle(true);
    return pipeliteLauncher;
  }

  @Test
  public void testFirstTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task("TASK1")
                      .executor(new ErrorTaskExecutor())
                      .taskDependsOnPrevious("TASK2")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK3")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK4")
                      .executor(new SuccessTaskExecutor())
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
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
  }

  @Test
  public void testSecondTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task("TASK1")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK2")
                      .executor(new ErrorTaskExecutor())
                      .taskDependsOnPrevious("TASK3")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK4")
                      .executor(new SuccessTaskExecutor())
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
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
  }

  @Test
  public void testThirdTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task("TASK1")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK2")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK3")
                      .executor(new ErrorTaskExecutor())
                      .taskDependsOnPrevious("TASK4")
                      .executor(new SuccessTaskExecutor())
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
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 2);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
  }

  @Test
  public void testFourthTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task("TASK1")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK2")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK3")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK4")
                      .executor(new ErrorTaskExecutor())
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
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 3);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
  }

  @Test
  public void testNoTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<Process> processes = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processes.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task("TASK1")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK2")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK3")
                      .executor(new SuccessTaskExecutor())
                      .taskDependsOnPrevious("TASK4")
                      .executor(new SuccessTaskExecutor())
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
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 4);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
  }
}
