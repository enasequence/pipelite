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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import pipelite.ApplicationConfiguration;
import pipelite.lock.LauncherInstanceLocker;
import pipelite.lock.LauncherInstanceOraclePackageLocker;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import pipelite.task.result.resolver.TaskExecutionResultResolver;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.configuration.LSFExecutorFactory;
import uk.ac.ebi.ena.sra.pipeline.configuration.PipeliteProcessFactory;
import pipelite.lock.ProcessInstanceOraclePackageLocker;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.TaskIdSource;
import pipelite.lock.ProcessInstanceLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleProcessIdSource;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleStorage;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;

public class Launcher {

  private final ApplicationConfiguration applicationConfiguration;

  public Launcher(ApplicationConfiguration applicationConfiguration) {
    this.applicationConfiguration = applicationConfiguration;
  }

  private static final int DEFAULT_ERROR_EXIT = 1;
  private static final int NORMAL_EXIT = 0;

  private static ProcessPoolExecutor init(
      int workers, StorageBackend storage, ProcessInstanceLocker locker) {
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
      }
    };
  }

  private OracleStorage initStorageBackend() throws ClassNotFoundException, SQLException {
    OracleStorage os = new OracleStorage();

    Connection connection = DefaultConfiguration.currentSet().createConnection();
    connection.setAutoCommit(false);

    os.setConnection(connection);
    os.setProcessTableName(DefaultConfiguration.currentSet().getProcessTableName());
    os.setStageTableName(DefaultConfiguration.currentSet().getStageTableName());
    os.setPipelineName(applicationConfiguration.launcherConfiguration.getProcessName());
    os.setLogTableName(DefaultConfiguration.currentSet().getLogTableName());
    return os;
  }

  private TaskIdSource initTaskIdSource(TaskExecutionResultExceptionResolver resolver)
      throws ClassNotFoundException, SQLException {
    OracleProcessIdSource ts = new OracleProcessIdSource();

    ts.setConnection(DefaultConfiguration.currentSet().createConnection());
    ts.setTableName(DefaultConfiguration.currentSet().getProcessTableName());
    ts.setPipelineName(applicationConfiguration.launcherConfiguration.getProcessName());
    ts.setRedoCount(applicationConfiguration.taskExecutorConfiguration.getRetries());
    ts.setExecutionResultArray(resolver.resultsArray());
    ts.init();

    return ts;
  }

  public void run(String... args) {
    try {
      System.exit(_run());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private int _run() {

    // TODO: get this from property
    TaskExecutionResultExceptionResolver resolver =
        TaskExecutionResultResolver.DEFAULT_EXCEPTION_RESOLVER;

    TaskIdSource task_id_source = null;
    PipeliteLauncher launcher = new PipeliteLauncher();
    OracleStorage storage = null;
    CountDownLatch latch = new CountDownLatch(1);

    String launcherName = applicationConfiguration.launcherConfiguration.getLauncherName();
    String processName = applicationConfiguration.launcherConfiguration.getProcessName();

    try (Connection connection = DefaultConfiguration.currentSet().createConnection()) {
      LauncherInstanceLocker launcherInstanceLocker =
          new LauncherInstanceOraclePackageLocker(connection);
      ProcessInstanceLocker processInstanceLocker =
          new ProcessInstanceOraclePackageLocker(connection);
      try {
        storage = initStorageBackend();

        if (launcherInstanceLocker.lock(launcherName, processName)) {
          task_id_source = initTaskIdSource(resolver);

          launcher.setTaskIdSource(task_id_source);
          launcher.setProcessFactory(
              new PipeliteProcessFactory(
                  launcherName, applicationConfiguration, resolver, processInstanceLocker));
          launcher.setExecutorFactory(
              new LSFExecutorFactory(
                  processName,
                  resolver,
                  applicationConfiguration.taskExecutorConfiguration,
                  applicationConfiguration.lsfTaskExecutorConfiguration));

          launcher.setSourceReadTimeout(120 * 1000);
          launcher.setProcessPool(
              init(
                  applicationConfiguration.launcherConfiguration.getWorkers(),
                  storage,
                  processInstanceLocker));

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
          launcherInstanceLocker.unlock(launcherName, processName);

        } else {
          System.out.println(
              String.format(
                  "Launcher %s is already locked for process %s.:", launcherName, processName));
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
