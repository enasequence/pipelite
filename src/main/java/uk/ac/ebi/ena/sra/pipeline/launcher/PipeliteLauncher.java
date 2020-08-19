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
import org.apache.log4j.Logger;
import pipelite.entity.PipeliteProcess;
import pipelite.process.state.ProcessExecutionState;
import pipelite.repository.PipeliteProcessRepository;
import pipelite.task.executor.TaskExecutor;

public class PipeliteLauncher {

  private final String processName;
  private final PipeliteProcessRepository pipeliteProcessRepository;

  public PipeliteLauncher(String processName, PipeliteProcessRepository pipeliteProcessRepository) {
    this.processName = processName;
    this.pipeliteProcessRepository = pipeliteProcessRepository;
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

  public interface StageExecutorFactory {
    TaskExecutor create();
  }

  TaggedPoolExecutor thread_pool;

  private ProcessFactory process_factory;
  private int source_read_timeout = 60 * 1000;
  private boolean exit_when_empty;
  private StageExecutorFactory executor_factory;
  private volatile boolean do_stop;
  private final Logger log = Logger.getLogger(this.getClass());

  public void setProcessFactory(ProcessFactory process_factory) {
    this.process_factory = process_factory;
  }

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
                    ? pipeliteProcessRepository.findAllByProcessNameAndStateOrderByPriorityDesc(
                        processName, ProcessExecutionState.ACTIVE)
                    : Collections.emptyList())) {
      if (exit_when_empty && pipeliteProcessQueue.isEmpty()) break;

      for (PipeliteProcess pipeliteProcess : pipeliteProcessQueue) {
        ProcessLauncherInterface process = getProcessFactory().create(pipeliteProcess);
        process.setExecutor(getExecutorFactory().create());
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

  ProcessFactory getProcessFactory() {
    return process_factory;
  }

  public void setExitWhenNoTasks(boolean exit_when_empty) {
    this.exit_when_empty = exit_when_empty;
  }

  public void setExecutorFactory(StageExecutorFactory executor_factory) {
    this.executor_factory = executor_factory;
  }

  public StageExecutorFactory getExecutorFactory() {
    return this.executor_factory;
  }
}
