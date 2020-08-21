/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import pipelite.RandomStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.executor.TaskExecutorFactory;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.service.PipeliteProcessService;
import pipelite.executor.TaskExecutor;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessLauncherInterface;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class LauncherTest {
  static final long delay = 60;
  static final int workers = ForkJoinPool.getCommonPoolParallelism();

  static final int PIPELITE_PROCESS_LIST_COUNT = 10;
  static final int PIPELITE_PROCESS_LIST_SIZE = 100;

  static int pipeliteProcessExecutionCount = 0;

  private ProcessConfiguration defaultProcessConfiguration() {
    return ProcessConfiguration.builder()
        .processName(RandomStringGenerator.randomProcessName())
        .resolver(DefaultExceptionResolver.NAME)
        .build();
  }

  private TaskConfiguration defaultTaskConfiguration() {
    return TaskConfiguration.builder().build();
  }

  @Test
  public void test() throws InterruptedException {

    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();

    PipeliteProcessService pipeliteProcessService = mock(PipeliteProcessService.class);

    doAnswer(
            new Answer<Object>() {
              int pipeliteProcessListCount = 0;

              public Object answer(InvocationOnMock invocation) {

                while (++pipeliteProcessListCount <= PIPELITE_PROCESS_LIST_COUNT) {
                  return IntStream.range(0, PIPELITE_PROCESS_LIST_SIZE)
                      .mapToObj(
                          i -> {
                            PipeliteProcess pipeliteProcess = new PipeliteProcess();
                            pipeliteProcess.setProcessId(RandomStringGenerator.randomProcessId());
                            pipeliteProcess.setProcessName(processConfiguration.getProcessName());
                            return pipeliteProcess;
                          })
                      .collect(Collectors.toList());
                }
                return null;
              }
            })
        .when(pipeliteProcessService)
        .getActiveProcesses(any());

    TaskExecutorFactory taskExecutorFactory = (a, b) -> null;

    PipeliteLauncher.ProcessFactory processFactory =
        pipeliteProcess ->
            new ProcessLauncherInterface() {
              @Override
              public void run() {
                System.out.println("EXECUTING " + pipeliteProcess.getProcessId());
                pipeliteProcessExecutionCount++;
                try {
                  Thread.sleep(delay);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }

              @Override
              public String getProcessId() {
                return pipeliteProcess.getProcessId();
              }

              @Override
              public TaskExecutor getExecutor() {
                return null;
              }

              @Override
              public PipeliteProcess getPipeliteProcess() {
                return null;
              }
            };

    ProcessPoolExecutor pool =
        new ProcessPoolExecutor(workers) {
          public void unwind(ProcessLauncherInterface process) {
            System.out.println("FINISHED " + process.getProcessId());
          }

          public void init(ProcessLauncherInterface process) {
            System.out.println("INIT     " + process.getProcessId());
          }
        };

    PipeliteLauncher pipeliteLauncher =
        new PipeliteLauncher(
            processConfiguration,
            taskConfiguration,
            pipeliteProcessService,
            processFactory,
            taskExecutorFactory);
    pipeliteLauncher.setSourceReadTimeout(1);
    pipeliteLauncher.setProcessPool(pool);

    pipeliteLauncher.execute();

    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.MINUTES);

    long finish = System.currentTimeMillis();

    // TODO: only PIPELITE_PROCESS_LIST_COUNT * workers tasks are completed

    /*
    assertThat(pipeliteProcessExecutionCount)
        .isEqualTo(PIPELITE_PROCESS_LIST_COUNT * PIPELITE_PROCESS_LIST_SIZE);

    assertThat(pool.getTaskCount())
        .isEqualTo(PIPELITE_PROCESS_LIST_COUNT * PIPELITE_PROCESS_LIST_SIZE);

    assertThat(pool.getCompletedTaskCount())
        .isEqualTo(PIPELITE_PROCESS_LIST_COUNT * PIPELITE_PROCESS_LIST_SIZE);
    */

    assertThat(pool.getActiveCount()).isEqualTo(0); // Threads should properly react to interrupt
    pool.shutdownNow();
  }
}
