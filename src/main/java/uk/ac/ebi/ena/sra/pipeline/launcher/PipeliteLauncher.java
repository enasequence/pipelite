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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.executor.TaskExecutorFactory;
import pipelite.service.PipeliteProcessService;
import pipelite.executor.TaskExecutor;

@Slf4j
public class PipeliteLauncher {

  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final ProcessFactory processFactory;
  private final TaskExecutorFactory taskExecutorFactory;

  public PipeliteLauncher(
      ProcessConfiguration processConfiguration,
      TaskConfiguration taskConfiguration,
      PipeliteProcessService pipeliteProcessService,
      ProcessFactory processFactory,
      TaskExecutorFactory taskExecutorFactory) {
    this.processConfiguration = processConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.processFactory = processFactory;
    this.taskExecutorFactory = taskExecutorFactory;
  }

  public interface ProcessFactory {
    ProcessLauncherInterface create(PipeliteProcess pipeliteProcess);
  }

  public interface ProcessLauncherInterface extends Runnable {
    String getProcessId();

    TaskExecutor getExecutor();

    PipeliteProcess getPipeliteProcess();

    default void setExecutor(TaskExecutor executor) {}

    default void stop() {}

    default boolean isStopped() {
      return false;
    }
  }

  TaggedPoolExecutor thread_pool;

  private int source_read_timeout = 60 * 1000;
  private boolean exit_when_empty;
  private volatile boolean do_stop;

  public void setProcessPool(ProcessPoolExecutor thread_pool) {
    this.thread_pool = thread_pool;
  }

  void shutdown() {
    if (null != thread_pool) {
      thread_pool.shutdown();
      thread_pool.running.forEach(
          (p, r) -> {
            log.info("Sending stop to " + p);
            ((ProcessLauncher) r).stop();
          });

      try {
        while (!thread_pool.awaitTermination(30, TimeUnit.SECONDS)) {
          log.info("Awaiting for completion of " + thread_pool.getActiveCount() + " threads ");
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void stop() {
    this.do_stop = true;
  }

  public boolean isStopped() {
    return this.do_stop;
  }

  public void execute() {
    List<PipeliteProcess> pipeliteProcessQueue;
    main:
    while (!do_stop
        && null
            != (pipeliteProcessQueue =
                (thread_pool.getCorePoolSize() - thread_pool.getActiveCount()) > 0
                    ? pipeliteProcessService.getActiveProcesses(
                        processConfiguration.getProcessName())
                    : Collections.emptyList())) {
      if (exit_when_empty && pipeliteProcessQueue.isEmpty()) break;

      for (PipeliteProcess pipeliteProcess : pipeliteProcessQueue) {
        ProcessLauncherInterface process = processFactory.create(pipeliteProcess);
        process.setExecutor(taskExecutorFactory.create(processConfiguration, taskConfiguration));
        try {
          thread_pool.execute(process);
        } catch (RejectedExecutionException ree) {
          break;
        }
      }

      long until = System.currentTimeMillis() + getSourceReadTimout();
      while (until > System.currentTimeMillis()) {
        try {
          Thread.sleep(1000);
          if (0 == thread_pool.getActiveCount() && !pipeliteProcessQueue.isEmpty()) break;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break main;
        }
      }
    }
  }

  public int getSourceReadTimout() {
    return source_read_timeout;
  }

  public void setSourceReadTimeout(int source_read_timeout_ms) {
    this.source_read_timeout = source_read_timeout_ms;
  }

  public void setExitWhenNoTasks(boolean exit_when_empty) {
    this.exit_when_empty = exit_when_empty;
  }
}
