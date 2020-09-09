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

import java.time.Duration;
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
import pipelite.process.ProcessBuilder;
import pipelite.process.ProcessInstance;
import pipelite.process.ProcessSource;

@SpringBootTest(
    classes = FullTestConfiguration.class,
    properties = {
      "pipelite.launcher.workers=5",
      "pipelite.launcher.runDelay=250ms",
      "pipelite.task.resolver=pipelite.resolver.DefaultExceptionResolver"
    })
@ContextConfiguration(initializers = OracleSuccessPipeliteLauncherTest.TestContextInitializer.class)
@ActiveProfiles(value = {"oracle-test"})
public class OracleFailingPipeliteLauncherTest {

  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();
  private static final int PROCESS_CNT = 5;
  private static final Duration SCHEDULER_DELAY = Duration.ofMillis(250);

  private PipeliteLauncher init(
      List<ProcessInstance> processInstances, ProcessSource processSource) {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);
    processConfiguration.setProcessSource(processSource);
    pipeliteLauncher.setShutdownPolicy(ShutdownPolicy.SHUTDOWN_IF_IDLE);
    return pipeliteLauncher;
  }

  @Test
  public void testFirstTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(UniqueStringGenerator.randomTaskName(), new ErrorTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .build());
            });

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    PipeliteLauncher pipeliteLauncher = init(processInstances, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }

  @Test
  public void testSecondTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new ErrorTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .build());
            });

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    PipeliteLauncher pipeliteLauncher = init(processInstances, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }

  @Test
  public void testThirdTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new ErrorTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .build());
            });

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    PipeliteLauncher pipeliteLauncher = init(processInstances, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 2);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }

  @Test
  public void testFourthTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new ErrorTaskExecutor())
                      .build());
            });

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    PipeliteLauncher pipeliteLauncher = init(processInstances, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 3);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }

  @Test
  public void testNoTaskFails() {
    processConfiguration.setProcessName(PROCESS_NAME);

    List<ProcessInstance> processInstances = new ArrayList<>();
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              processInstances.add(
                  new ProcessBuilder(PROCESS_NAME, UniqueStringGenerator.randomProcessId(), 9)
                      .task(UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .taskDependsOnPrevious(
                          UniqueStringGenerator.randomTaskName(), new SuccessTaskExecutor())
                      .build());
            });

    TestInMemoryProcessSource processSource = new TestInMemoryProcessSource(processInstances);
    PipeliteLauncher pipeliteLauncher = init(processInstances, processSource);
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processSource.getNewProcessInstances()).isEqualTo(0);
    assertThat(processSource.getReturnedProcessInstances()).isEqualTo(0);
    assertThat(processSource.getAcceptedProcessInstances()).isEqualTo(PROCESS_CNT);
    assertThat(processSource.getRejectedProcessInstances()).isEqualTo(0);

    assertThat(pipeliteLauncher.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT * 4);
    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
  }
}