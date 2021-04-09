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

import java.time.Duration;
import java.util.List;
import java.util.Set;
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
import pipelite.configuration.PipeliteConfiguration;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.service.InternalErrorService;
import pipelite.service.PipeliteLockerService;
import pipelite.service.PipeliteServices;

@Flogger
public class DefaultProcessRunnerPool implements ProcessRunnerPool {
  private final String serviceName;
  private final InternalErrorService internalErrorService;
  private final PipeliteLockerService pipeliteLockerService;
  private final Function<String, ProcessRunner> processRunnerSupplier;
  private final PipeliteMetrics pipeliteMetrics;
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final Duration executorServiceShutdownPeriod;

  @Value
  public static class ActiveProcessRunner {
    private final String pipelineName;
    private final String processId;
    @EqualsAndHashCode.Exclude private final ProcessRunner processRunner;
  }

  private final Set<ActiveProcessRunner> active = ConcurrentHashMap.newKeySet();

  public DefaultProcessRunnerPool(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      Function<String, ProcessRunner> processRunnerSupplier) {
    Assert.notNull(pipeliteConfiguration, "Missing configuration");
    Assert.notNull(pipeliteServices, "Missing services");
    Assert.notNull(processRunnerSupplier, "Missing process runner supplier");
    this.serviceName = pipeliteConfiguration.service().getName();
    this.internalErrorService = pipeliteServices.internalError();
    this.pipeliteLockerService = pipeliteServices.locker();
    this.processRunnerSupplier = processRunnerSupplier;
    this.pipeliteMetrics = pipeliteMetrics;
    this.executorServiceShutdownPeriod =
        pipeliteConfiguration.service().getShutdownPeriodWithMargin();
  }

  /**
   * Executes the process.
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
            if (!pipeliteLockerService.lockProcess(pipelineName, processId)) {
              return;
            }
            result = processRunner.runProcess(process);
          } catch (Exception ex) {
            // Catching exceptions here to allow other processes to continue execution.
            internalErrorService.saveInternalError(
                serviceName, pipelineName, processId, this.getClass(), ex);
          } finally {
            if (result == null) {
              result = new ProcessRunnerResult();
            }
            pipeliteMetrics
                .pipeline(pipelineName)
                .increment(process.getProcessEntity().getProcessState(), result);
            callback.accept(process, result);
            // Unlock process.
            pipeliteLockerService.unlockProcess(pipelineName, processId);
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
          executorServiceShutdownPeriod.getSeconds(), TimeUnit.SECONDS);
      active.forEach(
          a -> pipeliteLockerService.unlockProcess(a.getPipelineName(), a.getProcessId()));
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void terminateProcesses() {
    active.forEach(runner -> runner.getProcessRunner().terminate());
  }
}
