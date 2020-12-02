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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;

@Flogger
public class ProcessLauncherPool {
  private final PipeliteLocker locker;
  private final ProcessLauncherFactory processLauncherFactory;
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
   * @param locker the launcher locker
   * @param processLauncherFactory the factory to create process launchers given pipeline name and
   *     process
   */
  public ProcessLauncherPool(PipeliteLocker locker, ProcessLauncherFactory processLauncherFactory) {
    Assert.notNull(locker, "Missing locker");
    Assert.notNull(processLauncherFactory, "Missing process launcher factory");
    this.locker = locker;
    this.processLauncherFactory = processLauncherFactory;
    log.atInfo().log("Starting up process launcher pool");
  }

  /**
   * Executes the process and calls the callback once the execution has finished. The new process
   * state is available from the process.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param callback the callback called once the process execution has finished. The new process
   *     state is available from the process.
   */
  public void run(String pipelineName, Process process, ProcessLauncherPoolCallback callback) {
    String processId = process.getProcessId();
    // Create process launcher.
    ProcessLauncher processLauncher = processLauncherFactory.apply(pipelineName, process);
    ActiveLauncher activeLauncher = new ActiveLauncher(pipelineName, processId, processLauncher);
    active.add(activeLauncher);
    // Run process.
    executorService.execute(
        () -> {
          long processExecutionCount = 0;
          long processExceptionCount = 0;
          try {
            // Lock process.
            if (!locker.lockProcess(pipelineName, processId)) {
              return;
            }
            processLauncher.run();
            ++processExecutionCount;
          } catch (Exception ex) {
            ++processExceptionCount;
            log.atSevere().withCause(ex).log(
                "An unexpected exception was thrown when executing %s %s", pipelineName, processId);
          } finally {
            // Unlock process.
            locker.unlockProcess(pipelineName, processId);
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

  public void shutDown() {
    log.atInfo().log("Shutting down process launcher pool");
    executorService.shutdown();
    try {
      executorService.awaitTermination(
          PipeliteServiceManager.STOP_WAIT_MIN_SECONDS, TimeUnit.SECONDS);
      active.forEach(a -> locker.unlockProcess(a.getPipelineName(), a.getProcessId()));
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
    log.atInfo().log("Process launcher pool has been shut down");
  }
}
