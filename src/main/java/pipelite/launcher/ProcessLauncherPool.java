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
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.entity.LauncherLockEntity;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.service.LockService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Flogger
public class ProcessLauncherPool {
  private final LockService lockService;
  private final BiFunction<String, Process, ProcessLauncher> factory;
  private final LauncherLockEntity lock;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @Value
  public static class Result {
    private final long processExecutionCount;
    private final long processExceptionCount;
    private final long stageSuccessCount;
    private final long stageFailedCount;
  }

  @Value
  public class ActiveLauncher {
    private final String pipelineName;
    private final String processId;
    @EqualsAndHashCode.Exclude private final ProcessLauncher processLauncher;
  }

  private final Set<ActiveLauncher> active = ConcurrentHashMap.newKeySet();

  /**
   * Creates a process launcher pool.
   *
   * @param lockService lock service
   * @param factory a factory to create process launchers given pipeline name and process
   * @param lock launcher lock
   */
  public ProcessLauncherPool(
      LockService lockService,
      BiFunction<String, Process, ProcessLauncher> factory,
      LauncherLockEntity lock) {
    Assert.notNull(lockService, "Missing lock service");
    Assert.notNull(lock, "Missing launcher lock");
    Assert.notNull(factory, "Missing factory");
    this.lockService = lockService;
    this.lock = lock;
    this.factory = factory;
    logContext(log.atInfo()).log("Starting up process launcher pool");
  }

  /**
   * Executes the process and calls the callback once the execution has finished. The new process
   * state is available from the process.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param callback the callback is called once the process execution has finished. The new process
   *     state is available from the process.
   */
  public void run(String pipelineName, Process process, BiConsumer<Process, Result> callback) {
    String processId = process.getProcessId();
    // Create process launcher.
    ProcessLauncher processLauncher = factory.apply(pipelineName, process);
    ActiveLauncher activeLauncher = new ActiveLauncher(pipelineName, processId, processLauncher);
    active.add(activeLauncher);
    // Run process.
    executorService.execute(
        () -> {
          long processExecutionCount = 0;
          long processExceptionCount = 0;
          try {
            // Lock process.
            if (!lockService.lockProcess(lock, pipelineName, processId)) {
              return;
            }
            processLauncher.run();
            ++processExecutionCount;
          } catch (Exception ex) {
            ++processExceptionCount;
            logContext(log.atSevere(), pipelineName, processId)
                .withCause(ex)
                .log("Failed to execute process because an unexpected exception was thrown");
          } finally {
            // Unlock process.
            lockService.unlockProcess(lock, pipelineName, processId);
            active.remove(activeLauncher);
            Result result =
                new Result(
                    processExecutionCount,
                    processExceptionCount,
                    processLauncher.getStageSuccessCount(),
                    processLauncher.getStageFailedCount());
            callback.accept(process, result);
          }
        });
  }

  public boolean hasActivePipeline(String pipelineName) {
    return active.stream().anyMatch(p -> p.getPipelineName().equals(pipelineName));
  }

  public boolean hasActiveProcess(String pipelineName, String processId) {
    return active.stream()
        .anyMatch(
            p -> p.getPipelineName().equals(pipelineName) && p.getProcessId().equals(processId));
  }

  public long size() {
    return active.size();
  }

  public List<ProcessLauncher> get() {
    return active.stream().map(a -> a.getProcessLauncher()).collect(Collectors.toList());
  }

  public void shutDown() throws InterruptedException {
    logContext(log.atInfo()).log("Shutting down process launcher pool");
    executorService.shutdown();
    try {
      // TODO: service shutdown max time from launcher config
      executorService.awaitTermination(ServerManager.FORCE_STOP_WAIT_SECONDS - 1, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      throw ex;
    } finally {
      lockService.unlockProcesses(lock);
      lockService.unlockLauncher(lock);
      logContext(log.atInfo()).log("Process launcher pool has been shut down");
    }
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName, String processId) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, processId);
  }
}
