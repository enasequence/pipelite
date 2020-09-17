/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.launcher;

import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.AbstractScheduledService;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.locker.LauncherLocker;
import pipelite.launcher.locker.ProcessLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.service.LockService;
import pipelite.service.ProcessService;
import pipelite.service.StageService;

@Flogger
@Component
@Scope("prototype")
public class PipeliteLauncher extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessService processService;
  private final StageService stageService;
  private final LockService lockService;
  private final String launcherName;
  private final LauncherLocker launcherLocker;
  private final ProcessLocker processLocker;
  private final ExecutorService executorService;
  private final int workers;
  private final ProcessFactory processFactory;
  private ProcessSource processSource;

  private final AtomicInteger processFailedToCreateCount = new AtomicInteger(0);
  private final AtomicInteger processFailedToExecuteCount = new AtomicInteger(0);
  private final AtomicInteger processCompletedCount = new AtomicInteger(0);
  private final AtomicInteger stageFailedCount = new AtomicInteger(0);
  private final AtomicInteger stageCompletedCount = new AtomicInteger(0);

  private final Map<String, ProcessLauncher> initProcesses = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncher> activeProcesses = new ConcurrentHashMap<>();

  private final ArrayList<ProcessEntity> processQueue = new ArrayList<>();
  private int processQueueIndex = 0;
  private LocalDateTime processQueueValidUntil = LocalDateTime.now();

  private boolean shutdownIfIdle;
  private long iterations = 0;
  private Long maxIterations;

  public static final int DEFAULT_WORKERS = ForkJoinPool.getCommonPoolParallelism();
  public static final Duration DEFAULT_PROCESS_LAUNCH_FREQUENCY = Duration.ofMinutes(1);
  public static final Duration DEFAULT_PROCESS_PRIORITIZATION_FREQUENCY = Duration.ofHours(1);
  private final Duration processLaunchFrequency;
  private final Duration processPrioritizationFrequency;

  public PipeliteLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfiguration processConfiguration,
      @Autowired StageConfiguration stageConfiguration,
      @Autowired ProcessService processService,
      @Autowired StageService stageService,
      @Autowired LockService lockService) {
    if (!launcherConfiguration.validate()) {
      throw new IllegalArgumentException("Invalid launcher configuration");
    }

    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.lockService = lockService;
    this.launcherName = launcherConfiguration.getLauncherName();
    this.launcherLocker = new LauncherLocker(launcherName, lockService);
    this.processLocker = new ProcessLocker(launcherName, lockService);
    this.workers =
        launcherConfiguration.getWorkers() > 0
            ? launcherConfiguration.getWorkers()
            : DEFAULT_WORKERS;
    this.executorService = Executors.newFixedThreadPool(workers);

    if (launcherConfiguration.getProcessLaunchFrequency() != null) {
      this.processLaunchFrequency = launcherConfiguration.getProcessLaunchFrequency();
    } else {
      this.processLaunchFrequency = DEFAULT_PROCESS_LAUNCH_FREQUENCY;
    }

    if (launcherConfiguration.getProcessPrioritizationFrequency() != null) {
      this.processPrioritizationFrequency =
          launcherConfiguration.getProcessPrioritizationFrequency();
    } else {
      this.processPrioritizationFrequency = DEFAULT_PROCESS_PRIORITIZATION_FREQUENCY;
    }

    this.shutdownIfIdle = launcherConfiguration.isShutdownIfIdle();

    this.processFactory = ProcessConfiguration.getProcessFactory(processConfiguration);
    if (processFactory == null) {
      throw new IllegalArgumentException("Missing process factory");
    }
    if (processFactory.getPipelineName() == null) {
      throw new IllegalArgumentException("Missing process factory pipeline name");
    }
    if (processConfiguration.isProcessSource()) {
      this.processSource = ProcessConfiguration.getProcessSource(processConfiguration);
    } else {
      this.processSource = null;
    }
  }

  @Override
  public String serviceName() {
    return launcherName + "/" + getPipelineName();
  }

  @Override
  protected void startUp() {
    logContext(log.atInfo()).log("Starting up launcher");

    if (!launcherLocker.lock()) {
      throw new RuntimeException("Could not start launcher");
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processLaunchFrequency);
  }

  @Override
  protected void runOneIteration() throws Exception {
    if (!isRunning()) {
      return;
    }

    logContext(log.atInfo()).log("Running launcher");

    if (processQueueIndex >= processQueue.size()
        || processQueueValidUntil.isBefore(LocalDateTime.now())) {
      if (processConfiguration.isProcessSource()) {
        createNewProcesses();
      }
      queueProcesses();
    }

    while (processQueueIndex < processQueue.size() && initProcesses.size() < workers) {
      launchProcess();
    }

    stopIfMaxIterations();
    stopIfIdle();
  }

  private void stopIfMaxIterations() {
    if (maxIterations != null && ++iterations > maxIterations) {
      stopAsync();
    }
  }

  private void stopIfIdle() throws InterruptedException {
    if (processQueueIndex == processQueue.size() && shutdownIfIdle) {
      logContext(log.atInfo()).log("Shutting down no new active processes to launch");

      while (!initProcesses.isEmpty()) {
        try {
          Thread.sleep(Duration.ofSeconds(1).toMillis());
        } catch (InterruptedException ex) {
          throw ex;
        }
      }
      stopAsync();
    }
  }

  private void createNewProcesses() {
    logContext(log.atInfo()).log("Creating new process instances");

    while (true) {
      ProcessSource.NewProcess newProcess = processSource.next();
      if (newProcess == null) {
        break;
      }

      if (!validateNewProcess(newProcess)) {
        continue;
      }

      ProcessEntity newProcessEntity =
          ProcessEntity.newExecution(
              newProcess.getProcessId().trim(), getPipelineName(), newProcess.getPriority());

      processService.saveProcess(newProcessEntity);

      processSource.accept(newProcess.getProcessId());
    }
  }

  private boolean validateNewProcess(ProcessSource.NewProcess newProcess) {
    String processId = newProcess.getProcessId();
    if (processId == null || processId.trim().isEmpty()) {
      logContext(log.atWarning()).log("Could not create process instance without a process id");
      processFailedToCreateCount.incrementAndGet();
      return false;
    }

    processId = processId.trim();

    Optional<ProcessEntity> savedProcessEntity =
        processService.getSavedProcess(getPipelineName(), processId);

    if (savedProcessEntity.isPresent()) {
      logContext(log.atSevere(), processId)
          .log("Could not create process instance with a process id that already exists");
      processFailedToCreateCount.incrementAndGet();
      processSource.reject(processId);
      return false;
    }

    return true;
  }

  private void queueProcesses() {

    logContext(log.atInfo()).log("Queuing process instances");

    // Clear process queue.
    processQueueIndex = 0;
    processQueue.clear();

    // First add active processes in case we can recover them.
    processQueue.addAll(
        processService.getActiveProcesses(getPipelineName()).stream()
            .filter(processEntity -> !activeProcesses.containsKey(processEntity.getProcessId()))
            .collect(Collectors.toList()));

    // Then add new processes.
    processQueue.addAll(
        processService.getNewProcesses(getPipelineName()).stream()
            .filter(processEntity -> !activeProcesses.containsKey(processEntity.getProcessId()))
            .collect(Collectors.toList()));
    processQueueValidUntil = LocalDateTime.now().plus(processPrioritizationFrequency);
  }

  private void launchProcess() {
    ProcessEntity processEntity = processQueue.get(processQueueIndex++);
    String processId = processEntity.getProcessId();

    logContext(log.atInfo(), processId).log("Launching process instances");

    ProcessLauncher processLauncher =
        new ProcessLauncher(
            launcherConfiguration, stageConfiguration, processService, stageService);

    Process process = processFactory.create(processId);

    if (process == null) {
      logContext(log.atSevere(), processId).log("Could not create process instance");
      processFailedToCreateCount.incrementAndGet();
      return;
    }

    if (!validateProcess(process)) {
      logContext(log.atSevere(), processId).log("Failed to validate process instance");
      processFailedToCreateCount.incrementAndGet();
      return;
    }

    processLauncher.init(process, processEntity);
    initProcesses.put(processId, processLauncher);

    executorService.execute(
        () -> {
          activeProcesses.put(processId, processLauncher);
          try {
            if (!processLocker.lock(getPipelineName(), processId)) {
              return;
            }
            processLauncher.run();
            processCompletedCount.incrementAndGet();
          } catch (Exception ex) {
            processFailedToExecuteCount.incrementAndGet();
            logContext(log.atSevere(), processId).withCause(ex).log("Failed to execute process");
          } finally {
            processLocker.unlock(getPipelineName(), processId);
            activeProcesses.remove(processId);
            initProcesses.remove(processId);
            stageCompletedCount.addAndGet(processLauncher.getStageCompletedCount());
            stageFailedCount.addAndGet(processLauncher.getStageFailedCount());
          }
        });
  }

  private boolean validateProcess(Process process) {
    if (process == null) {
      return false;
    }

    boolean isSuccess = process.validate(Process.ValidateMode.WITHOUT_STAGES);

    if (!getPipelineName().equals(process.getPipelineName())) {
      process
          .logContext(log.atSevere())
          .log("Pipeline name is different from launcher pipeline name: %s", getPipelineName());
      isSuccess = false;
    }

    return isSuccess;
  }

  @Override
  protected void shutDown() throws Exception {
    logContext(log.atInfo()).log("Shutting down launcher");

    executorService.shutdown();
    try {
      executorService.awaitTermination(ServerManager.FORCE_STOP_WAIT_SECONDS - 1, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      throw ex;
    } finally {
      launcherLocker.unlock();

      logContext(log.atInfo()).log("Launcher has been shut down");
    }
  }

  public String getPipelineName() {
    return processFactory.getPipelineName();
  }

  public int getActiveProcessCount() {
    return activeProcesses.size();
  }

  public int getProcessFailedToCreateCount() {
    return processFailedToCreateCount.get();
  }

  public int getProcessFailedToExecuteCount() {
    return processFailedToExecuteCount.get();
  }

  public int getProcessCompletedCount() {
    return processCompletedCount.get();
  }

  public int getStageFailedCount() {
    return stageFailedCount.get();
  }

  public int getStageCompletedCount() {
    return stageCompletedCount.get();
  }

  public boolean isShutdownIfIdle() {
    return shutdownIfIdle;
  }

  public void setShutdownIfIdle(boolean shutdownIfIdle) {
    this.shutdownIfIdle = shutdownIfIdle;
  }

  public Long getMaxIterations() {
    return maxIterations;
  }

  public void setMaxIterations(Long maxIterations) {
    this.maxIterations = maxIterations;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PIPELINE_NAME, getPipelineName());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PIPELINE_NAME, getPipelineName())
        .with(LogKey.PROCESS_ID, processId);
  }
}
