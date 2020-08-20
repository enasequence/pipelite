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
import pipelite.entity.PipeliteProcess;
import pipelite.service.PipeliteProcessService;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessLauncherInterface;
import uk.ac.ebi.ena.sra.pipeline.launcher.ProcessPoolExecutor;
import pipelite.executor.TaskExecutor;

import javax.transaction.Transactional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class LauncherDBTest {

  @Autowired PipeliteProcessService pipeliteProcessService;

  static final long delay = 5 * 1000;
  static final int workers = ForkJoinPool.getCommonPoolParallelism();

  @Test
  @Transactional
  @Rollback
  public void test() throws InterruptedException {

    PipeliteLauncher.ProcessFactory pr_src =
        pipeliteProcess ->
            new ProcessLauncherInterface() {
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

    PipeliteLauncher l =
        new PipeliteLauncher(RandomStringGenerator.randomProcessName(), pipeliteProcessService);
    l.setSourceReadTimeout(1);
    l.setProcessFactory(pr_src);
    l.setProcessPool(pool);
    l.setExitWhenNoTasks(true);

    long start = System.currentTimeMillis();
    l.execute();

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
