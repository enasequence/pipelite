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
package pipelite.launcher.process.runner;

import com.google.common.flogger.FluentLogger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.launcher.PipeliteServiceManager;
import pipelite.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;

@Flogger
public class DefaultProcessRunnerPool implements ProcessRunnerPool {
  private final PipeliteLocker pipeliteLocker;
  private final Function<String, ProcessRunner> processRunnerSupplier;
  private final PipeliteMetrics metrics;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @Value
  public static class ActiveProcessRunner {
    private final String pipelineName;
    private final String processId;
    @EqualsAndHashCode.Exclude private final ProcessRunner processRunner;
  }

  private final Set<ActiveProcessRunner> active = ConcurrentHashMap.newKeySet();

  public DefaultProcessRunnerPool(
      PipeliteLocker pipeliteLocker,
      Function<String, ProcessRunner> processRunnerSupplier,
      PipeliteMetrics metrics) {
    Assert.notNull(pipeliteLocker, "Missing pipelite locker");
    Assert.notNull(processRunnerSupplier, "Missing process runner supplier");
    Assert.notNull(metrics, "Missing pipelite metrics");
    this.pipeliteLocker = pipeliteLocker;
    this.processRunnerSupplier = processRunnerSupplier;
    this.metrics = metrics;
  }

  /**
   * Executes the process. Exceptions thrown by {@link
   * pipelite.launcher.process.runner.ProcessRunner#runProcess} are caught and logged and the
   * process runner callback is called after incrementing the internal error count of the returned
   * ProcessRunnerResult.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param callback the process runner callback
   */
  @Override
  public void runProcess(String pipelineName, Process process, ProcessRunnerCallback callback) {

    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessId(), "Missing process id");
    Assert.notNull(callback, "Missing process runner callback");

    String processId = process.getProcessId();

    // Create process launcher.
    ProcessRunner processRunner = processRunnerSupplier.apply(pipelineName);
    ActiveProcessRunner activeProcessRunner =
        new ActiveProcessRunner(pipelineName, processId, processRunner);
    active.add(activeProcessRunner);
    // Run process.
    executorService.execute(
        () -> {
          ProcessRunnerResult result = null;
          try {
            // Lock process.
            if (!pipeliteLocker.lockProcess(pipelineName, processId)) {
              return;
            }
            result = processRunner.runProcess(process);
          } catch (Exception ex) {
            logContext(log.atSevere(), pipelineName, processId)
                .withCause(ex)
                .log("Unexpected exception when executing process");
          } finally {
            if (result == null) {
              result = new ProcessRunnerResult().incrementInternalError();
            }
            metrics
                .pipeline(pipelineName)
                .increment(process.getProcessEntity().getProcessState(), result);
            callback.accept(process, result);
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

  public int getActiveProcessCount() {
    return active.size();
  }

  public List<ProcessRunner> getActiveProcessRunners() {
    return active.stream().map(ActiveProcessRunner::getProcessRunner).collect(Collectors.toList());
  }

  @Override
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

  @Override
  public void terminate() {
    active.forEach(runner -> runner.getProcessRunner().terminate());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName, String processId) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, processId);
  }
}
