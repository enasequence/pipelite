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
  private AtomicInteger launchedProcessCount = new AtomicInteger(0);
  private AtomicInteger declinedProcessCount = new AtomicInteger(0);
  private AtomicInteger completedProcessCount = new AtomicInteger(0);
  private final Map<String, ProcessLauncher> activeProcesses = new ConcurrentHashMap<>();

  private int refreshTimeoutHours = 1;
  private int launchTimeoutMilliseconds = 15 * 1000;
  private int stopIfEmptyTimeoutMilliseconds = 1000;

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

    String launcherName = launcherConfiguration.getLauncherName();
    String processName = processConfiguration.getProcessName();

    while (!stop) {

      if (activeProcessQueueIndex == activeProcessQueue.size()
          || activeProcessQueueTimeout.isBefore(LocalDateTime.now())) {

        log.info(
            "Launcher {} for process {} is getting new active processes",
            launcherName,
            processName);
        activeProcessQueue = pipeliteProcessService.getActiveProcesses(processName);
        activeProcessQueueIndex = 0;
        activeProcessQueueTimeout = LocalDateTime.now().plusHours(refreshTimeoutHours);
      }

      if (activeProcessQueueIndex == activeProcessQueue.size() && stopIfEmpty) {
        log.info(
            "Launcher {} for process {} is stopping as there are no new active processes",
            launcherName,
            processName);
        while (launchedProcessCount.get() > completedProcessCount.get()) {
          sleep(stopIfEmptyTimeoutMilliseconds);
        }
        stop();
        return;
      }

      while (activeProcessQueueIndex < activeProcessQueue.size()
          && activeProcesses.size() < launcherConfiguration.getWorkers()) {

        PipeliteProcess pipeliteProcess = activeProcessQueue.get(activeProcessQueueIndex++);
        String processId = pipeliteProcess.getProcessId();

        ProcessLauncher processLauncher = processLauncherFactory.create(pipeliteProcess);

        log.info(
            "Launcher {} for process {} is launching process id {}",
            launcherName,
            processName,
            processId);

        launchedProcessCount.incrementAndGet();

        executorService.execute(
            () -> {
              activeProcesses.put(processId, processLauncher);
              try {
                if (!processLauncher.call()) {
                  declinedProcessCount.incrementAndGet();
                }
              } catch (Exception ex) {
                log.error(
                    "Exception thrown by launcher {} process {} process id {}",
                    launcherName,
                    processName,
                    processId,
                    ex);
              } finally {
                activeProcesses.remove(processId);
                completedProcessCount.incrementAndGet();
              }
            });
      }

      log.info(
          "Launcher {} for process {} has now {} active process executions",
          launcherName,
          processName,
          activeProcesses.size());

      sleep(launchTimeoutMilliseconds);
    }
  }

  private void sleep(int miliseconds) {
    try {
      Thread.sleep(miliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public int getActiveProcessCount() {
    return activeProcesses.size();
  }

  public int getLaunchedProcessCount() {
    return launchedProcessCount.get();
  }

  public int getDeclinedProcessCount() {
    return declinedProcessCount.get();
  }

  public int getCompletedProcessCount() {
    return completedProcessCount.get();
  }

  public int getRefreshTimeoutHours() {
    return refreshTimeoutHours;
  }

  public void setRefreshTimeoutHours(int hours) {
    this.refreshTimeoutHours = hours;
  }

  public int getLaunchTimeoutMilliseconds() {
    return launchTimeoutMilliseconds;
  }

  public void setLaunchTimeoutMilliseconds(int milliseconds) {
    this.launchTimeoutMilliseconds = milliseconds;
  }

  public int getStopIfEmptyTimeoutMilliseconds() {
    return stopIfEmptyTimeoutMilliseconds;
  }

  public void setStopIfEmptyTimeoutMilliseconds(int stopIfEmptyTimeoutMilliseconds) {
    this.stopIfEmptyTimeoutMilliseconds = stopIfEmptyTimeoutMilliseconds;
  }
}
