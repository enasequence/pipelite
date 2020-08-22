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
package pipelite.launcher;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.service.PipeliteProcessService;
import pipelite.process.launcher.ProcessLauncher;
import pipelite.process.launcher.ProcessLauncherFactory;

@Slf4j
public class PipeliteLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final ProcessLauncherFactory processLauncherFactory;
  private final ExecutorService executorService;
  private AtomicInteger submittedProcessCount = new AtomicInteger(0);
  private AtomicInteger completedProcessCount = new AtomicInteger(0);
  private final Map<String, ProcessLauncher> activeProcesses = new ConcurrentHashMap<>();

  private static final int ACTIVE_PROCESS_QUEUE_TIMEOUT_HOURS = 1;

  public PipeliteLauncher(
      LauncherConfiguration launcherConfiguration,
      ProcessConfiguration processConfiguration,
      PipeliteProcessService pipeliteProcessService,
      ProcessLauncherFactory processLauncherFactory) {
    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.processLauncherFactory = processLauncherFactory;
    this.executorService = Executors.newFixedThreadPool(launcherConfiguration.getWorkers());
  }

  private volatile boolean stop;
  private boolean stopIfEmpty;

  public void stop() {
    this.stop = true;
    executorService.shutdown();
    for (ProcessLauncher processLauncher : activeProcesses.values()) {
      processLauncher.stop();
    }
    try {
      executorService.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public void stopIfEmpty() {
    this.stopIfEmpty = true;
  }

  public void execute() {
    List<PipeliteProcess> activeProcessQueue = Collections.emptyList();
    int activeProcessQueueIndex = 0;
    LocalDateTime activeProcessQueueTimeout = LocalDateTime.now();

    while (!stop) {

      if (activeProcessQueueIndex == activeProcessQueue.size()
          || activeProcessQueueTimeout.isBefore(LocalDateTime.now())) {
        activeProcessQueue =
            pipeliteProcessService.getActiveProcesses(processConfiguration.getProcessName());
        activeProcessQueueIndex = 0;
        activeProcessQueueTimeout =
            LocalDateTime.now().plusHours(ACTIVE_PROCESS_QUEUE_TIMEOUT_HOURS);
      }

      if (activeProcessQueueIndex == activeProcessQueue.size() && stopIfEmpty) {
        while (submittedProcessCount.get() > completedProcessCount.get()) {
          sleepOneSecond();
        }
        log.info(
            "Stopping pipelite.launcher {} for process {} as there are no more tasks",
            launcherConfiguration.getLauncherName(),
            processConfiguration.getProcessName());
        stop();
        return;
      }

      while (activeProcessQueueIndex < activeProcessQueue.size()
          && activeProcesses.size() < launcherConfiguration.getWorkers()) {
        ProcessLauncher processLauncher =
            processLauncherFactory.create(activeProcessQueue.get(activeProcessQueueIndex++));
        submittedProcessCount.incrementAndGet();
        executorService.execute(
            () -> {
              String processId = processLauncher.getProcessId();
              activeProcesses.put(processId, processLauncher);
              try {
                processLauncher.run();
              } catch (Exception ex) {
                log.error(
                    "Exception from pipelite.launcher {} for process {}",
                    launcherConfiguration.getLauncherName(),
                    processLauncher.getProcessId(),
                    ex);
                throw ex;
              } finally {
                activeProcesses.remove(processId);
                completedProcessCount.incrementAndGet();
              }
            });
      }

      sleepOneSecond();
    }
  }

  private void sleepOneSecond() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public int getActiveProcessCount() {
    return activeProcesses.size();
  }

  public int getSubmittedProcessCount() {
    return submittedProcessCount.get();
  }

  public int getCompletedProcessCount() {
    return completedProcessCount.get();
  }
}
