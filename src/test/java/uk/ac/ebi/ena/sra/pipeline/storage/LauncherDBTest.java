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

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.RandomStringGenerator;
import pipelite.TestConfiguration;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.service.PipeliteProcessService;
import pipelite.launcher.PipeliteLauncher;
import pipelite.process.launcher.ProcessLauncher;
import pipelite.process.launcher.ProcessLauncherFactory;

import javax.transaction.Transactional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class LauncherDBTest {

  @Autowired PipeliteProcessService pipeliteProcessService;

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

  @Test
  @Transactional
  @Rollback
  public void test() {

    LauncherConfiguration launcherConfiguration = defaultLauncherConfiguration();
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();

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

    PipeliteLauncher pipeliteLauncher =
        new PipeliteLauncher(
            launcherConfiguration,
            processConfiguration,
            pipeliteProcessService,
            processLauncherFactory);

    pipeliteLauncher.stopIfEmpty();

    long start = System.currentTimeMillis();

    pipeliteLauncher.execute();

    long finish = System.currentTimeMillis();

    System.out.println(
        "Completed: "
            + pipeliteLauncher.getCompletedProcessCount()
            + " for "
            + (finish - start)
            + " mS using "
            + workers
            + " thread(s)");
    System.out.println("CPU count: " + Runtime.getRuntime().availableProcessors());
    System.out.println("Available parallelism: " + ForkJoinPool.getCommonPoolParallelism());

    assertEquals(
        0, pipeliteLauncher.getActiveProcessCount());
  }
}
