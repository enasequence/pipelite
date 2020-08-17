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

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pipelite.entity.PipeliteProcess;
import pipelite.task.executor.TaskExecutor;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessLauncherInterface;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.StageExecutorFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LauncherTest {
  static final long delay = 5 * 1000;
  static final int workers = ForkJoinPool.getCommonPoolParallelism();
  static final Logger log = Logger.getLogger(LauncherTest.class);

  @BeforeAll
  public static void setup() {
    PropertyConfigurator.configure("resource/test.log4j.properties");
  }

  @Test
  public void main() throws SQLException, InterruptedException {
    PipeliteLauncher.TaskIdSource id_src =
        new PipeliteLauncher.TaskIdSource() {
          int index = 10;

          @Override
          public List<String> getTaskQueue() {
            for (index--; index > 0; )
              return Stream.iterate(0, i -> ++i)
                  .limit(2000)
                  .map(i -> String.format("TEST%06d", i))
                  .collect(Collectors.toList());
            return null;
          }
        };

    StageExecutorFactory e_src = () -> null;

    PipeliteLauncher.ProcessFactory pr_src =
        process_id ->
            new ProcessLauncherInterface() {
              @Override
              public void run() {
                System.out.println("EXECUTING " + process_id);
                try {
                  Thread.sleep(delay);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                //                        if( ThreadLocalRandom.current().nextDouble() > 0.5 )
                //                            throw new RuntimeException();
                //                        else
                throw new Error();
              }

              @Override
              public String getProcessId() {
                return process_id;
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

    PipeliteLauncher l = new PipeliteLauncher();
    l.setTaskIdSource(id_src);
    l.setSourceReadTimeout(1);
    l.setProcessFactory(pr_src);
    l.setExecutorFactory(e_src);

    l.setProcessPool(pool);

    long start = System.currentTimeMillis();
    l.execute();

    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.MINUTES);

    long finish = System.currentTimeMillis();
    long task_speed = (finish - start) / pool.getCompletedTaskCount() * workers;

    log.info(
        "Completed: "
            + pool.getCompletedTaskCount()
            + " for "
            + (finish - start)
            + " mS using "
            + workers
            + " thread(s)");
    log.info("Task speed: " + task_speed + " mS/task ");
    log.info("CPU count: " + Runtime.getRuntime().availableProcessors());
    log.info("Available parallelism: " + ForkJoinPool.getCommonPoolParallelism());

    assertEquals(0, pool.getActiveCount()); // Threads should properly react to interrupt
    pool.shutdownNow();
    assertTrue(task_speed - delay < 3000); // Performance degradation?
  }
}
