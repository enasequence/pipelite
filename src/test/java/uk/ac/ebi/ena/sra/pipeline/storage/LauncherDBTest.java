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
package uk.ac.ebi.ena.sra.pipeline.storage;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.RandomStringGenerator;
import pipelite.TestConfiguration;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.executor.TaskExecutorFactory;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.service.PipeliteLockService;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher;
import uk.ac.ebi.ena.sra.pipeline.launcher.ProcessLauncher;
import uk.ac.ebi.ena.sra.pipeline.launcher.ProcessLauncherFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.ProcessPoolExecutor;
import pipelite.executor.TaskExecutor;

import javax.transaction.Transactional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class LauncherDBTest {

  @Autowired PipeliteProcessService pipeliteProcessService;
  @Autowired PipeliteStageService pipeliteStageService;
  @Autowired PipeliteLockService pipeliteLockService;

  static final long delay = 5 * 1000;
  static final int workers = ForkJoinPool.getCommonPoolParallelism();

  private LauncherConfiguration defaultLauncherConfiguration() {
    return LauncherConfiguration.builder().launcherName("TEST_LAUNCHER").build();
  }

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
  @Transactional
  @Rollback
  public void test() throws InterruptedException {

    LauncherConfiguration launcherConfiguration = defaultLauncherConfiguration();
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();
    TaskConfiguration taskConfiguration = defaultTaskConfiguration();

    ProcessLauncherFactory processLauncherFactory =
        pipeliteProcess ->
            new ProcessLauncher() {
              @Override
              public String getProcessId() {
                return pipeliteProcess.getProcessId();
              }

              @Override
              public void run() {
                System.out.println("EXECUTING " + pipeliteProcess.getProcessId());
                try {
                  Thread.sleep(delay);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                throw new Error();
              }

              @Override
              public void stop() {}
            };

    ProcessPoolExecutor pool =
        new ProcessPoolExecutor(workers) {
          public void unwind(ProcessLauncher process) {}

          public void init(ProcessLauncher process) {}
        };

    PipeliteLauncher pipeliteLauncher =
        new PipeliteLauncher(
            launcherConfiguration,
            processConfiguration,
            taskConfiguration,
            pipeliteProcessService,
            pipeliteStageService,
            pipeliteLockService,
            processLauncherFactory);
    pipeliteLauncher.setSourceReadTimeout(1);
    pipeliteLauncher.setProcessPool(pool);
    pipeliteLauncher.setExitWhenNoTasks(true);

    long start = System.currentTimeMillis();
    pipeliteLauncher.execute();

    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.MINUTES);

    long finish = System.currentTimeMillis();

    System.out.println(
        "Completed: "
            + pool.getCompletedTaskCount()
            + " for "
            + (finish - start)
            + " mS using "
            + workers
            + " thread(s)");
    System.out.println("CPU count: " + Runtime.getRuntime().availableProcessors());
    System.out.println("Available parallelism: " + ForkJoinPool.getCommonPoolParallelism());

    assertEquals(0, pool.getActiveCount()); // Threads should properly react to interrupt
    pool.shutdownNow();
  }
}
