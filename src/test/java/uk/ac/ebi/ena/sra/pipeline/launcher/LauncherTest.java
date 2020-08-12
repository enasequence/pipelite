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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.StageExecutorFactory;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;

public class LauncherTest {
  static final long delay = 5 * 1000;
  static final int workers = ForkJoinPool.getCommonPoolParallelism();
  static Logger log = Logger.getLogger(LauncherTest.class);

  @BeforeClass
  public static void setup() {
    PropertyConfigurator.configure("resource/test.log4j.properties");
  }

  @Test
  public void main()
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException,
          InterruptedException {
    PipeliteLauncher.TaskIdSource id_src =
        new PipeliteLauncher.TaskIdSource() {
          int index = 10;

          @Override
          public List<String> getTaskQueue() throws SQLException {
            for (index--; index > 0; )
              return Stream.iterate(0, i -> ++i)
                  .limit(2000)
                  .map(i -> String.format("TEST%06d", i))
                  .collect(Collectors.toList());
            return null;
          }
        };

    StageExecutorFactory e_src =
        new StageExecutorFactory() {
          @Override
          public StageExecutor getExecutor() {
            return null;
          }
        };

    PipeliteLauncher.ProcessFactory pr_src =
        new PipeliteLauncher.ProcessFactory() {
          @Override
          public PipeliteProcess getProcess(String process_id) {
            return new PipeliteProcess() {
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
              public StageExecutor getExecutor() {
                return null;
              }
            };
          }
        };

    ProcessPoolExecutor pool =
        new ProcessPoolExecutor(workers) {
          public void unwind(PipeliteProcess process) {
            System.out.println("FINISHED " + process.getProcessId());
          }

          public void init(PipeliteProcess process) {
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

    Assert.assertTrue(0 == pool.getActiveCount()); // Threads should properly react to interrupt
    pool.shutdownNow();
    Assert.assertTrue(task_speed - delay < 3000); // Performance degradation?
  }

  public void setProcessID(String process_id) {}

  public StorageBackend getStorage() {
    return null;
  }

  public void setStorage(StorageBackend storage) {}

  public void setExecutor(StageExecutor executor) {}
}
