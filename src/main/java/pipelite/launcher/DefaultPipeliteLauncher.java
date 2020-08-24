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
import java.util.stream.Collectors;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.log.LogKey;
import pipelite.service.PipeliteLockService;
import pipelite.service.PipeliteProcessService;

@Flogger
@Component
public class DefaultPipeliteLauncher implements PipeliteLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteLockService pipeliteLockService;
  private final ExecutorService executorService;

  private boolean lock;
  private volatile boolean stop;
  private boolean stopIfEmpty;

  private AtomicInteger initProcessCount = new AtomicInteger(0);
  private AtomicInteger declinedProcessCount = new AtomicInteger(0);
  private AtomicInteger completedProcessCount = new AtomicInteger(0);
  private final Map<String, ProcessLauncher> initProcesses = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncher> activeProcesses = new ConcurrentHashMap<>();

  private int refreshTimeoutHours = 1;
  private int launchTimeoutMilliseconds = 15 * 1000;
  private int stopIfEmptyTimeoutMilliseconds = 1000;

  @Autowired private ObjectProvider<ProcessLauncher> processLauncherObjectProvider;

  public DefaultPipeliteLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfiguration processConfiguration,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteLockService pipeliteLockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.pipeliteLockService = pipeliteLockService;

    Integer workers = launcherConfiguration.getWorkers();
    if (workers == null || workers > ForkJoinPool.getCommonPoolParallelism()) {
      workers = ForkJoinPool.getCommonPoolParallelism();
    }
    this.executorService = Executors.newFixedThreadPool(workers);
  }

  public boolean init() {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Initialising launcher");

    return lockLauncher();
  }

  public void execute() {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Executing launcher");

    List<String> activeProcessQueue = Collections.emptyList();
    int activeProcessQueueIndex = 0;
    LocalDateTime activeProcessQueueTimeout = LocalDateTime.now();

    String launcherName = getLauncherName();
    String processName = getProcessName();

    while (!stop) {

      if (activeProcessQueueIndex == activeProcessQueue.size()
          || activeProcessQueueTimeout.isBefore(LocalDateTime.now())) {

        log.atInfo()
            .with(LogKey.LAUNCHER_NAME, launcherName)
            .with(LogKey.PROCESS_NAME, processName)
            .log("Retrieving active processes to launch");

        activeProcessQueue =
            pipeliteProcessService.getActiveProcesses(processName).stream()
                .map(pipeliteProcess -> pipeliteProcess.getProcessId())
                .collect(Collectors.toList());
        activeProcessQueueIndex = 0;
        activeProcessQueueTimeout = LocalDateTime.now().plusHours(refreshTimeoutHours);
      }

      if (activeProcessQueueIndex == activeProcessQueue.size() && stopIfEmpty) {
        log.atInfo()
            .with(LogKey.LAUNCHER_NAME, launcherName)
            .with(LogKey.PROCESS_NAME, processName)
            .log("No new active processes to launch");

        while (initProcessCount.get() > completedProcessCount.get()) {
          sleep(stopIfEmptyTimeoutMilliseconds);
        }
        stop();
        return;
      }

      while (activeProcessQueueIndex < activeProcessQueue.size()
          && activeProcesses.size() < launcherConfiguration.getWorkers()) {

        // Launch new process execution

        String processId = activeProcessQueue.get(activeProcessQueueIndex++);

        log.atInfo()
            .with(LogKey.LAUNCHER_NAME, launcherName)
            .with(LogKey.PROCESS_NAME, processName)
            .with(LogKey.PROCESS_ID, processId)
            .log("Creating process launcher");

        ProcessLauncher processLauncher = processLauncherObjectProvider.getObject();

        if (!processLauncher.init(processId)) {
          log.atWarning()
              .with(LogKey.LAUNCHER_NAME, launcherName)
              .with(LogKey.PROCESS_NAME, processName)
              .with(LogKey.PROCESS_ID, processId)
              .log("Failed to initialise process launcher");

          declinedProcessCount.incrementAndGet();
          continue;
        } else {
          initProcesses.put(processId, processLauncher);
          initProcessCount.incrementAndGet();
        }

        log.atInfo()
            .with(LogKey.LAUNCHER_NAME, launcherName)
            .with(LogKey.PROCESS_NAME, processName)
            .with(LogKey.PROCESS_ID, processId)
            .log("Executing process launcher");

        executorService.execute(
            () -> {
              activeProcesses.put(processId, processLauncher);
              try {
                processLauncher.execute();
              } catch (Exception ex) {
                log.atSevere()
                    .with(LogKey.LAUNCHER_NAME, launcherName)
                    .with(LogKey.PROCESS_NAME, processName)
                    .with(LogKey.PROCESS_ID, processId)
                    .withCause(ex);
              } finally {
                processLauncher.close();
                initProcesses.remove(processId);
                activeProcesses.remove(processId);
                completedProcessCount.incrementAndGet();
              }
            });
      }

      sleep(launchTimeoutMilliseconds);
    }
  }

  private boolean lockLauncher() {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Attempting to lock launcher");

    if (pipeliteLockService.lockLauncher(getLauncherName(), getProcessName())) {
      lock = true;
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, getLauncherName())
          .with(LogKey.PROCESS_NAME, getProcessName())
          .log("Locked launcher");
      return true;
    }
    log.atWarning()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Failed to lock launcher");
    return false;
  }

  private void unlockLauncher() {
    if (!lock) {
      return;
    }
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Attempting to unlock launcher");

    if (pipeliteLockService.unlockLauncher(getLauncherName(), getProcessName())) {
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, getLauncherName())
          .with(LogKey.PROCESS_NAME, getProcessName())
          .log("Unlocked launcher");
      lock = false;
    } else {
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, getLauncherName())
          .with(LogKey.PROCESS_NAME, getProcessName())
          .log("Failed to unlocked launcher");
    }
  }

  @Override
  public void stop() {
    if (!stop) {
      return;
    }
    this.stop = true;

    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Stopping launcher");

    executorService.shutdown();
    for (ProcessLauncher processLauncher : initProcesses.values()) {
      processLauncher.stop();
    }
    try {
      executorService.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    } finally {
      unlockLauncher();

      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, getLauncherName())
          .with(LogKey.PROCESS_NAME, getProcessName())
          .log("Launcher stopped");
    }
  }

  @Override
  public void setStopIfEmpty() {
    this.stopIfEmpty = true;
  }

  private void sleep(int miliseconds) {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Launcher sleeping for %s milliseconds", miliseconds);
    try {
      Thread.sleep(miliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public String getLauncherName() {
    return launcherConfiguration.getLauncherName();
  }

  public String getProcessName() {
    return processConfiguration.getProcessName();
  }

  public int getActiveProcessCount() {
    return activeProcesses.size();
  }

  public int getInitProcessCount() {
    return initProcessCount.get();
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
