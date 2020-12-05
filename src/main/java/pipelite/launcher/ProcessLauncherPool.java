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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;

@Flogger
public class ProcessLauncherPool implements ProcessRunner {
  private final PipeliteLocker pipeliteLocker;
  private final Supplier<ProcessLauncher> processLauncherSupplier;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

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
   * @param processLauncherSupplier the process launcher supplier
   */
  public ProcessLauncherPool(
      PipeliteLocker pipeliteLocker, Supplier<ProcessLauncher> processLauncherSupplier) {
    Assert.notNull(processLauncherSupplier, "Missing pipelite locker");
    Assert.notNull(processLauncherSupplier, "Missing process launcher supplier");
    this.pipeliteLocker = pipeliteLocker;
    this.processLauncherSupplier = processLauncherSupplier;
  }

  @Override
  public void runProcess(String pipelineName, Process process, Collection<ProcessRunnerCallback> callbacks) {
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(callbacks, "Missing callback");
    String processId = process.getProcessId();
    // Create process launcher.
    ProcessLauncher processLauncher = processLauncherSupplier.get();
    ActiveLauncher activeLauncher = new ActiveLauncher(pipelineName, processId, processLauncher);
    active.add(activeLauncher);
    // Run process.
    executorService.execute(
        () -> {
          long processExecutionCount = 0;
          long processExceptionCount = 0;
          try {
            // Lock process.
            if (!pipeliteLocker.lockProcess(pipelineName, processId)) {
              return;
            }
            processLauncher.run(pipelineName, process);
            ++processExecutionCount;
          } catch (Exception ex) {
            ++processExceptionCount;
            logContext(log.atSevere(), pipelineName, processId)
                .withCause(ex)
                .log("Unexpected exception");
          } finally {
            // Unlock process.
            try {
              ProcessRunnerResult result =
                  new ProcessRunnerResult(
                      processExecutionCount,
                      processExceptionCount,
                      processLauncher.getStageSuccessCount(),
                      processLauncher.getStageFailedCount());
              callbacks.stream().forEach(callback -> callback.accept(process, result));
            } finally {
              pipeliteLocker.unlockProcess(pipelineName, processId);
              active.remove(activeLauncher);
            }
          }
        });
  }

  @Override
  public boolean isPipelineActive(String pipelineName) {
    return active.stream().anyMatch(p -> p.getPipelineName().equals(pipelineName));
  }

  @Override
  public boolean isProcessActive(String pipelineName, String processId) {
    return active.stream()
        .anyMatch(
            p -> p.getPipelineName().equals(pipelineName) && p.getProcessId().equals(processId));
  }

  public int activeProcessCount() {
    return active.size();
  }

  public List<ProcessLauncher> get() {
    return active.stream().map(a -> a.getProcessLauncher()).collect(Collectors.toList());
  }

  public void shutDown() {
    executorService.shutdown();
    try {
      executorService.awaitTermination(
          PipeliteServiceManager.STOP_WAIT_MIN_SECONDS, TimeUnit.SECONDS);
      active.forEach(a -> pipeliteLocker.unlockProcess(a.getPipelineName(), a.getProcessId()));
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName, String processId) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, processId);
  }
}
