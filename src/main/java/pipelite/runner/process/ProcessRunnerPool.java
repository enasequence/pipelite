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
package pipelite.runner.process;

import com.google.common.util.concurrent.AbstractScheduledService;
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

/** Abstract base class for executing processes. */
@Flogger
public class ProcessRunnerPool extends AbstractScheduledService {

  private final InternalErrorService internalErrorService;
  private final PipeliteLockerService pipeliteLockerService;
  private final Function<String, ProcessRunner> processRunnerSupplier;
  private final PipeliteMetrics pipeliteMetrics;
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final Duration executorServiceShutdownPeriod;
  private final Duration processRunnerFrequency;
  private final String serviceName;
  private Runnable runnerCallback;

  @Value
  public static class ActiveProcessRunner {
    private final String pipelineName;
    private final String processId;
    @EqualsAndHashCode.Exclude private final ProcessRunner processRunner;
  }

  private final Set<ProcessRunnerPool.ActiveProcessRunner> active = ConcurrentHashMap.newKeySet();

  public ProcessRunnerPool(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      String serviceName,
      Function<String, ProcessRunner> processRunnerSupplier) {
    Assert.notNull(pipeliteConfiguration, "Missing configuration");
    Assert.notNull(pipeliteServices, "Missing services");
    Assert.notNull(processRunnerSupplier, "Missing process runner supplier");
    this.internalErrorService = pipeliteServices.internalError();
    this.pipeliteLockerService = pipeliteServices.locker();
    this.pipeliteMetrics = pipeliteMetrics;
    this.serviceName = serviceName;
    this.processRunnerSupplier = processRunnerSupplier;
    this.executorServiceShutdownPeriod =
        pipeliteConfiguration.service().getShutdownPeriodWithMargin();
    this.processRunnerFrequency = pipeliteConfiguration.advanced().getProcessRunnerFrequency();
  }

  /**
   * Callback called once every iteration of the process runner pool.
   *
   * @param runnerCallback callback called once every iteration of the process runner pool
   */
  public void setRunnerCallback(Runnable runnerCallback) {
    this.runnerCallback = runnerCallback;
  }

  // From AbstractScheduledService.
  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processRunnerFrequency);
  }

  @Override
  protected void startUp() {
    log.atInfo().log("Starting up service: %s", serviceName());
  }

  // From AbstractScheduledService.
  @Override
  public void runOneIteration() {
    if (runnerCallback == null) {
      log.atWarning().log("No process runner pool callback: %s", serviceName());
    }
    log.atFine().log("Calling process runner pool callback: %s", serviceName());

    try {
      this.runnerCallback.run();
      pipeliteMetrics.setRunningProcessesCount(getActiveProcessRunners());
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception from service: %s", serviceName());
    }

    if (getActiveProcessCount() == 0 && shutdownIfIdle()) {
      log.atInfo().log("Stopping idle process runner pool: %s", serviceName());
      stopAsync();
    }
  }

  /**
   * Override to allow an idle process runner pool to be shut down.
   *
   * @return true if an idle process runner pool should be shut down
   */
  protected boolean shutdownIfIdle() {
    return false;
  }

  /**
   * Executes the process.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param callback the process runner callback
   */
  public void runProcess(String pipelineName, Process process, ProcessRunnerCallback callback) {

    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessId(), "Missing process id");
    Assert.notNull(callback, "Missing process runner callback");

    String processId = process.getProcessId();

    // Create process runner.
    ProcessRunner processRunner = processRunnerSupplier.apply(pipelineName);
    ProcessRunnerPool.ActiveProcessRunner activeProcessRunner =
        new ProcessRunnerPool.ActiveProcessRunner(pipelineName, processId, processRunner);
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

  /**
   * Returns true if the pipeline is active.
   *
   * @param pipelineName the pipeline name
   * @return true if the pipeline is active.
   */
  public boolean isPipelineActive(String pipelineName) {
    return active.stream().anyMatch(p -> p.getPipelineName().equals(pipelineName));
  }

  /**
   * Returns true if the process is active.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if the process is active.
   */
  public boolean isProcessActive(String pipelineName, String processId) {
    return active.stream()
        .anyMatch(
            p -> p.getPipelineName().equals(pipelineName) && p.getProcessId().equals(processId));
  }

  /**
   * Returns the number of active process runners.
   *
   * @return the number of active process runners
   */
  public int getActiveProcessCount() {
    return active.size();
  }

  /**
   * Returns the active process runners.
   *
   * @return the active process runners
   */
  public List<ProcessRunner> getActiveProcessRunners() {
    return active.stream()
        .map(ProcessRunnerPool.ActiveProcessRunner::getProcessRunner)
        .collect(Collectors.toList());
  }

  /** Terminates running processes. */
  public void terminateProcesses() {
    active.forEach(runner -> runner.getProcessRunner().terminate());
  }

  /** Shuts down the process runner pool. */
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
}
