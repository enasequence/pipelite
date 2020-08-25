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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.AbstractScheduledService;
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
public class DefaultPipeliteLauncher extends AbstractScheduledService implements PipeliteLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteLockService pipeliteLockService;
  private final ExecutorService executorService;

  private final AtomicInteger processInitCount = new AtomicInteger(0);
  private final AtomicInteger processStartFailureCount = new AtomicInteger(0);
  private final AtomicInteger processRejectCount = new AtomicInteger(0);
  private final AtomicInteger processRunFailureCount = new AtomicInteger(0);
  private final AtomicInteger processCompletedCount = new AtomicInteger(0);
  private final AtomicInteger taskFailedCount = new AtomicInteger(0);
  private final AtomicInteger taskSkippedCount = new AtomicInteger(0);
  private final AtomicInteger taskCompletedCount = new AtomicInteger(0);

  private final Map<String, ProcessLauncher> initProcesses = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncher> activeProcesses = new ConcurrentHashMap<>();

  @Autowired private ObjectProvider<ProcessLauncher> processLauncherObjectProvider;

  public enum ShutdownPolicy {
    SHUTDOWN_IF_IDLE,
    WAIT_IF_IDLE
  };

  private ShutdownPolicy shutdownPolicy = ShutdownPolicy.WAIT_IF_IDLE;

  private List<String> activeProcessQueue = Collections.emptyList();
  private int activeProcessQueueIndex = 0;
  private int activeProcessQueueValidHours = 1;
  private LocalDateTime activeProcessQueueValidUntil = LocalDateTime.now();

  private int schedulerDelayMillis = 1000;
  private int stopDelayMillis = 1000;

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

  @Override
  public String serviceName() {
    return getLauncherName() + "/" + getProcessName();
  }

  @Override
  protected void startUp() {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Starting up launcher");

    if (!lockLauncher()) {
      throw new RuntimeException("Could not start process launcher");
    }

    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Launcher has been started up");
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, Duration.ofMillis(schedulerDelayMillis));
  }

  @Override
  protected void runOneIteration() throws Exception {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Running launcher");

    String launcherName = getLauncherName();
    String processName = getProcessName();

    if (activeProcessQueueIndex == activeProcessQueue.size()
        || activeProcessQueueValidUntil.isBefore(LocalDateTime.now())) {

      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .log("Finding active processes to launch");

      activeProcessQueue =
          pipeliteProcessService.getActiveProcesses(processName).stream()
              .map(pipeliteProcess -> pipeliteProcess.getProcessId())
              .collect(Collectors.toList());
      activeProcessQueueIndex = 0;
      activeProcessQueueValidUntil = LocalDateTime.now().plusHours(activeProcessQueueValidHours);
    }

    if (activeProcessQueueIndex == activeProcessQueue.size()
        && ShutdownPolicy.SHUTDOWN_IF_IDLE.equals(shutdownPolicy)) {
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .log("Shutting down no new active processes to launch");

      while (!initProcesses.isEmpty()) {
        try {
          Thread.sleep(stopDelayMillis);
        } catch (InterruptedException ex) {
          throw ex;
        }
      }
      stopAsync();
      return;
    }

    while (activeProcessQueueIndex < activeProcessQueue.size()
        && initProcesses.size() < launcherConfiguration.getWorkers()) {

      // Launch new process execution

      String processId = activeProcessQueue.get(activeProcessQueueIndex++);

      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .log("Starting process launcher");

      ProcessLauncher processLauncher = processLauncherObjectProvider.getObject();
      processLauncher.init(processId);

      initProcesses.put(processId, processLauncher);
      processInitCount.incrementAndGet();

      executorService.execute(
          () -> {
            activeProcesses.put(processId, processLauncher);
            try {
              try {
                processLauncher.startAsync().awaitRunning();
              } catch (IllegalStateException ex) {
                log.atWarning()
                    .with(LogKey.LAUNCHER_NAME, launcherName)
                    .with(LogKey.PROCESS_NAME, processName)
                    .with(LogKey.PROCESS_ID, processId)
                    .withCause(processLauncher.failureCause())
                    .log("Failed to start process launcher");
                processStartFailureCount.incrementAndGet();
              }

              try {
                processLauncher.awaitTerminated();
                processCompletedCount.incrementAndGet();
              } catch (IllegalStateException ex) {
                if (processLauncher.failureCause()
                    instanceof DefaultProcessLauncher.ProcessNotExecutableException) {
                  log.atWarning()
                      .with(LogKey.LAUNCHER_NAME, launcherName)
                      .with(LogKey.PROCESS_NAME, processName)
                      .with(LogKey.PROCESS_ID, processId)
                      .log("Process was not executable");
                  processRejectCount.incrementAndGet();
                } else {
                  log.atSevere()
                      .with(LogKey.LAUNCHER_NAME, launcherName)
                      .with(LogKey.PROCESS_NAME, processName)
                      .with(LogKey.PROCESS_ID, processId)
                      .withCause(processLauncher.failureCause())
                      .log("Failed to run process launcher");
                  processRunFailureCount.incrementAndGet();
                }
              }
            } finally {
              activeProcesses.remove(processId);
              initProcesses.remove(processId);
              taskCompletedCount.addAndGet(processLauncher.getTaskCompletedCount());
              taskSkippedCount.addAndGet(processLauncher.getTaskSkippedCount());
              taskFailedCount.addAndGet(processLauncher.getTaskFailedCount());
            }
          });
    }
  }

  @Override
  protected void shutDown() throws Exception {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Shutting down launcher");

    executorService.shutdown();
    try {
      executorService.awaitTermination(
          PipeliteLauncherServiceManager.FORCE_STOP_WAIT_SECONDS - 1, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      throw ex;
    } finally {
      unlockLauncher();

      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, getLauncherName())
          .with(LogKey.PROCESS_NAME, getProcessName())
          .log("Launcher has been shut down");
    }
  }

  private boolean lockLauncher() {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Attempting to lock launcher");

    if (pipeliteLockService.lockLauncher(getLauncherName(), getProcessName())) {
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
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .log("Attempting to unlock launcher");

    if (pipeliteLockService.unlockLauncher(getLauncherName(), getProcessName())) {
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, getLauncherName())
          .with(LogKey.PROCESS_NAME, getProcessName())
          .log("Unlocked launcher");
    } else {
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, getLauncherName())
          .with(LogKey.PROCESS_NAME, getProcessName())
          .log("Failed to unlocked launcher");
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

  public int getProcessInitCount() {
    return processInitCount.get();
  }

  public int getProcessStartFailureCount() {
    return processStartFailureCount.get();
  }

  public int getProcessRejectCount() {
    return processRejectCount.get();
  }

  public int getProcessRunFailureCount() {
    return processRunFailureCount.get();
  }

  public int getProcessCompletedCount() {
    return processCompletedCount.get();
  }

  public int getTaskFailedCount() {
    return taskFailedCount.get();
  }

  public int getTaskSkippedCount() {
    return taskSkippedCount.get();
  }

  public int getTaskCompletedCount() {
    return taskCompletedCount.get();
  }

  public int getActiveProcessQueueValidHours() {
    return activeProcessQueueValidHours;
  }

  public void setActiveProcessQueueValidHours(int hours) {
    this.activeProcessQueueValidHours = hours;
  }

  public int getSchedulerDelayMillis() {
    return schedulerDelayMillis;
  }

  public void setSchedulerDelayMillis(int milliseconds) {
    this.schedulerDelayMillis = milliseconds;
  }

  public ShutdownPolicy getShutdownPolicy() {
    return shutdownPolicy;
  }

  public void setShutdownPolicy(ShutdownPolicy shutdownPolicy) {
    this.shutdownPolicy = shutdownPolicy;
  }
}
