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

import java.util.concurrent.CountDownLatch;

import lombok.AllArgsConstructor;
import pipelite.ApplicationConfiguration;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.service.PipeliteLockService;
import uk.ac.ebi.ena.sra.pipeline.configuration.LSFExecutorFactory;
import uk.ac.ebi.ena.sra.pipeline.configuration.PipeliteProcessFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessLauncherInterface;

@AllArgsConstructor
public class Launcher {

  private final ApplicationConfiguration applicationConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService locker;

  private static final int DEFAULT_ERROR_EXIT = 1;
  private static final int NORMAL_EXIT = 0;

  private static ProcessPoolExecutor init(int workers) {
    return new ProcessPoolExecutor(workers) {
      public void unwind(ProcessLauncherInterface process) {}

      public void init(ProcessLauncherInterface process) {}
    };
  }

  public void run(String... args) {
    try {
      System.exit(_run());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private int _run() {

    CountDownLatch latch = new CountDownLatch(1);

    String launcherName = applicationConfiguration.launcherConfiguration.getLauncherName();
    String processName = applicationConfiguration.launcherConfiguration.getProcessName();

    PipeliteLauncher launcher = new PipeliteLauncher(processName, pipeliteProcessService);

    try {
      if (locker.lockLauncher(launcherName, processName)) {

        launcher.setProcessFactory(
            new PipeliteProcessFactory(
                launcherName,
                applicationConfiguration,
                locker,
                pipeliteProcessService,
                pipeliteStageService));
        launcher.setExecutorFactory(
            new LSFExecutorFactory(
                processName,
                applicationConfiguration.processConfiguration.createResolver(),
                applicationConfiguration.processConfiguration,
                applicationConfiguration.taskExecutorConfiguration,
                applicationConfiguration.lsfTaskExecutorConfiguration));

        launcher.setSourceReadTimeout(120 * 1000);
        launcher.setProcessPool(init(applicationConfiguration.launcherConfiguration.getWorkers()));

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
        locker.unlockLauncher(launcherName, processName);

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
      latch.countDown();
    }
  }
}
