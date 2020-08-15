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

import com.beust.jcommander.JCommander;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall.LSFQueue;
import uk.ac.ebi.ena.sra.pipeline.base.external.lsf.LSFBqueues;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.configuration.LSFExecutorFactory;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultLauncherParams;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultProcessFactory;
import uk.ac.ebi.ena.sra.pipeline.dblock.DBLockManager;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.TaskIdSource;
import uk.ac.ebi.ena.sra.pipeline.resource.ResourceLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleProcessIdSource;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleStorage;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;

public class Launcher {

  private static final int DEFAULT_ERROR_EXIT = 1;
  private static final int NORMAL_EXIT = 0;

  private static ProcessPoolExecutor init(
      int workers, StorageBackend storage, ResourceLocker locker) {
    return new ProcessPoolExecutor(workers) {
      public void unwind(PipeliteProcess process) {
        StorageBackend storage = process.getStorage();
        try {
          storage.flush();
        } catch (StorageException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      public void init(PipeliteProcess process) {
        process.setStorage(storage);
        process.setLocker(locker);
      }
    };
  }

  private static OracleStorage initStorageBackend()
      throws ClassNotFoundException, SQLException {
    OracleStorage os = new OracleStorage();

    Connection connection = DefaultConfiguration.currentSet().createConnection();
    connection.setAutoCommit(false);

    os.setConnection(connection);
    os.setProcessTableName(DefaultConfiguration.currentSet().getProcessTableName());
    os.setStageTableName(DefaultConfiguration.currentSet().getStageTableName());
    os.setPipelineName(DefaultConfiguration.currentSet().getPipelineName());
    os.setLogTableName(DefaultConfiguration.currentSet().getLogTableName());
    return os;
  }

  private static TaskIdSource initTaskIdSource(TaskExecutionResultExceptionResolver resolver)
      throws ClassNotFoundException, SQLException {
    OracleProcessIdSource ts = new OracleProcessIdSource();

    ts.setConnection(DefaultConfiguration.currentSet().createConnection());
    ts.setTableName(DefaultConfiguration.currentSet().getProcessTableName());
    ts.setPipelineName(DefaultConfiguration.currentSet().getPipelineName());
    ts.setRedoCount(DefaultConfiguration.currentSet().getStagesRedoCount());
    ts.setExecutionResultArray(resolver.resultsArray());
    ts.init();

    return ts;
  }

  public static void main(String[] args)
      throws IOException {
    DefaultLauncherParams params = new DefaultLauncherParams();
    JCommander jc = new JCommander(params);

    try {
      jc.parse(args);

      // Extra checks for existing queues
      if (null != params.queue_name
          && params.queue_name.trim().length() > 0
          && !"default".equals(params.queue_name.trim())) {
        Set<String> queues = new LSFBqueues().executeAndGet();
        if (!queues.contains(params.queue_name)) {
          System.out.printf("Supplied queue \"%s\" is missing.\n", params.queue_name);
          System.out.println("Available queues: ");
          for (LSFQueue q : LSFQueue.values()) System.out.printf("\t%s\n", q.getQueueName());

          System.exit(DEFAULT_ERROR_EXIT);
        }
      }

    } catch (Exception e) {
      System.out.println("**");
      jc.usage();
      System.exit(DEFAULT_ERROR_EXIT);
    }

    TaskExecutionResultExceptionResolver resolver = DefaultConfiguration.CURRENT.getResolver();

    System.exit(main2(resolver, params));
  }

  private static int main2(TaskExecutionResultExceptionResolver resolver, DefaultLauncherParams params) throws IOException {
    EnhancedPatternLayout layout =
        new EnhancedPatternLayout(
            "%d{ISO8601} %-5p [%t] "
                + DefaultConfiguration.currentSet().getPipelineName()
                + " %c{1}:%L - %m%n");
    DailyRollingFileAppender appender =
        new DailyRollingFileAppender(layout, params.log_file, "'.'yyyy-ww");
    appender.setThreshold(Level.ALL);
    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(appender);
    Logger.getRootLogger().setLevel(Level.ALL);

    TaskIdSource task_id_source = null;
    PipeliteLauncher launcher = new PipeliteLauncher();
    OracleStorage storage = null;
    CountDownLatch latch = new CountDownLatch(1);

    try (Connection connection = DefaultConfiguration.currentSet().createConnection()) {
      try (LauncherLockManager lockman =
          new DBLockManager(connection, DefaultConfiguration.currentSet().getPipelineName())) {
        storage = initStorageBackend();

        if (lockman.tryLock(params.lock)) {
          task_id_source = initTaskIdSource(resolver);

          launcher.setTaskIdSource(task_id_source);
          launcher.setProcessFactory(new DefaultProcessFactory(resolver));
          launcher.setExecutorFactory(
              new LSFExecutorFactory(
                  DefaultConfiguration.currentSet().getPipelineName(),
                  resolver,
                  params.queue_name,
                  params.lsf_mem,
                  params.lsf_cpu_cores,
                  params.lsf_mem_timeout
              ));

          launcher.setSourceReadTimeout(120 * 1000);
          launcher.setProcessPool(init(params.workers, storage, (ResourceLocker) lockman));

          // TODO remove
          Runtime.getRuntime()
              .addShutdownHook(
                  new Thread(
                      new Runnable() {
                        final Thread t = Thread.currentThread();

                        @Override
                        public void run() {
                          launcher.stop();
                          System.out.println(
                              t.getName()
                                  + " Stop requested from "
                                  + Thread.currentThread().getName());
                          try {
                            latch.await();
                            t.interrupt();
                            System.out.println(t.getName() + " exited");
                          } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                          }
                        }
                      }));

          launcher.execute();
          // TODO: check that all processes unlocks themselves
          lockman.unlock(params.lock);

        } else {
          System.out.println(
              String.format(
                  "another instance of %s is already running %s",
                  Launcher.class.getName(),
                  Files.exists(Paths.get(params.lock))
                      ? Files.readAllLines(Paths.get(params.lock))
                      : params.lock));
          return DEFAULT_ERROR_EXIT;
        }

        return NORMAL_EXIT;
      } catch (Throwable e) {
        e.printStackTrace();
        return DEFAULT_ERROR_EXIT;

      } finally {
        try {
          launcher.shutdown();
        } catch (Throwable t) {
          t.printStackTrace();
        }

        try {
          if (task_id_source instanceof OracleProcessIdSource)
            ((OracleProcessIdSource) task_id_source).done();
        } catch (Throwable t) {
          t.printStackTrace();
        }

        try {
          storage.flush();
        } catch (StorageException e) {
          e.printStackTrace();
        }

        try {
          storage.close();
        } catch (StorageException e) {
          e.printStackTrace();
        }

        latch.countDown();
      }

    } catch (Throwable e) {
      e.printStackTrace();
      return DEFAULT_ERROR_EXIT;
    }
  }
}
