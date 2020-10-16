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

import lombok.Data;
import org.springframework.beans.factory.ObjectProvider;
import pipelite.TestInMemoryProcessFactory;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.ErrorStageExecutor;
import pipelite.executor.SuccessStageExecutor;
import pipelite.process.Process;
import pipelite.process.ProcessSource;
import pipelite.process.builder.ProcessBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Data
public class PipeliteLauncherFailureTester {

  private final ProcessConfiguration processConfiguration;
  private final ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  private static final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private static final int PROCESS_CNT = 5;

  private PipeliteLauncher init(List<Process> processes, ProcessSource processSource) {
    TestInMemoryProcessFactory processFactory =
        new TestInMemoryProcessFactory(PIPELINE_NAME, processes);
    processConfiguration.setProcessFactory(processFactory);
    processConfiguration.setProcessSource(processSource);
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    pipeliteLauncher.setShutdownIfIdle(true);
    return pipeliteLauncher;
  }

  public void testFirstStageFails() {

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

  public void testSecondStageFails() {

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

  public void testThirdStageFails() {

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

  public void testFourthStageFails() {

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

  public void testNoStageFails() {

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
