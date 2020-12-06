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
public class DefaultProcessRunnerPool implements ProcessRunnerPool {
  private final PipeliteLocker pipeliteLocker;
  private final Supplier<ProcessRunner> processRunnerSupplier;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @Value
  public class ActiveProcessRunner {
    private final String pipelineName;
    private final String processId;
    @EqualsAndHashCode.Exclude private final ProcessRunner processRunner;
  }

  private final Set<ActiveProcessRunner> active = ConcurrentHashMap.newKeySet();

  /**
   * Creates a default process runner pool.
   *
   * @param processRunnerSupplier the process runner supplier
   */
  public DefaultProcessRunnerPool(
      PipeliteLocker pipeliteLocker, Supplier<ProcessRunner> processRunnerSupplier) {
    Assert.notNull(pipeliteLocker, "Missing pipelite locker");
    Assert.notNull(processRunnerSupplier, "Missing process runner supplier");
    this.pipeliteLocker = pipeliteLocker;
    this.processRunnerSupplier = processRunnerSupplier;
  }

  @Override
  public void runProcess(String pipelineName, Process process, ProcessRunnerCallback callback) {
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(callback, "Missing process runner callback");
    String processId = process.getProcessId();
    // Create process launcher.
    ProcessRunner processRunner = processRunnerSupplier.get();
    ActiveProcessRunner activeProcessRunner =
        new ActiveProcessRunner(pipelineName, processId, processRunner);
    active.add(activeProcessRunner);
    // Run process.
    executorService.execute(
        () -> {
          try {
            // Lock process.
            if (!pipeliteLocker.lockProcess(pipelineName, processId)) {
              return;
            }
            processRunner.runProcess(pipelineName, process, callback);
          } catch (Exception ex) {
            logContext(log.atSevere(), pipelineName, processId)
                .withCause(ex)
                .log("Process runner exception");
            ProcessRunnerResult result = new ProcessRunnerResult();
            result.setProcessExceptionCount(1);
            callback.accept(process, result);
          } finally {
            // Unlock process.
            pipeliteLocker.unlockProcess(pipelineName, processId);
            active.remove(activeProcessRunner);
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

  public int getActiveProcessRunnerCount() {
    return active.size();
  }

  public List<ProcessRunner> getActiveProcessRunners() {
    return active.stream().map(a -> a.getProcessRunner()).collect(Collectors.toList());
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
