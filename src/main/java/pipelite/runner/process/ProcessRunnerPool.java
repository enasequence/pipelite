/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.error.InternalErrorHandler;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.service.PipeliteServices;
import pipelite.time.Time;

/** Abstract base class for executing processes. */
@Flogger
public class ProcessRunnerPool extends AbstractScheduledService {

  private final PipeliteServices pipeliteServices;
  private final PipeliteMetrics pipeliteMetrics;
  private final String serviceName;
  private final ProcessRunnerFactory processRunnerFactory;
  private final Duration processRunnerFrequency;
  private final boolean shutdownIfIdle;
  private final Set<ProcessRunnerPool.ActiveProcessRunner> active = ConcurrentHashMap.newKeySet();
  private final InternalErrorHandler internalErrorHandler;
  private ZonedDateTime lastRunOneIteration = ZonedDateTime.now();
  private static final Duration timeSeriesUpdateFrequency = Duration.ofSeconds(15);

  @Data
  public static class ActiveProcessRunner {
    private final String pipelineName;
    private final String processId;
    @EqualsAndHashCode.Exclude private final ProcessRunnerResultCallback resultCallback;
    @EqualsAndHashCode.Exclude private ProcessRunner processRunner;
    @EqualsAndHashCode.Exclude private Future<?> future;
  }

  public ProcessRunnerPool(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      String serviceName,
      ProcessRunnerFactory processRunnerFactory) {
    Assert.notNull(pipeliteConfiguration, "Missing configuration");
    Assert.notNull(pipeliteServices, "Missing services");
    Assert.notNull(processRunnerFactory, "Missing process runner factory");
    this.pipeliteServices = pipeliteServices;
    this.pipeliteMetrics = pipeliteMetrics;
    this.serviceName = serviceName;
    this.processRunnerFactory = processRunnerFactory;
    this.processRunnerFrequency = pipeliteConfiguration.advanced().getProcessRunnerFrequency();
    this.shutdownIfIdle = pipeliteConfiguration.advanced().isShutdownIfIdle();
    this.internalErrorHandler =
        new InternalErrorHandler(pipeliteServices.internalError(), serviceName, this);
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
    // Unexpected exceptions are logged as internal errors but otherwise ignored to
    // keep process runner pool alive.
    internalErrorHandler.execute(() -> runProcessRunnerPool());
  }

  private void runProcessRunnerPool() {
    ZonedDateTime runOneIterationStartTime = ZonedDateTime.now();

    // ProcessRunnerPool runOneIteration is called from AbstractScheduledService schedule.
    // It is guaranteed not to be called concurrently. We use an executor service to call
    // ProcessRunner runOneIteration. The ProcessRunner runOneIteration will not execute
    // stages
    // in the same thread and should complete fairly quickly. We capture the ProcessRunner
    // runOneIteration future to make sure not to call ProcessRunner runOneIteration again
    // until the future has completed.

    runOneIterationForActiveProcessRunners();

    if (lastRunOneIteration.plus(timeSeriesUpdateFrequency).isBefore(ZonedDateTime.now())) {
      lastRunOneIteration = ZonedDateTime.now();
      List<ProcessRunner> activeProcessRunners = getActiveProcessRunners();
      pipeliteMetrics.setRunningProcessesCount(activeProcessRunners, lastRunOneIteration);
      pipeliteMetrics.setRunningStagesCount(activeProcessRunners, lastRunOneIteration);
      pipeliteMetrics.setSubmittedStagesCount(activeProcessRunners, lastRunOneIteration);
    }

    if (shutdownIfIdle && isIdle()) {
      log.atInfo().log("Stopping idle process runner pool: %s", serviceName());
      stopAsync();
    }
    pipeliteMetrics.processRunnerPool().endRunOneIteration(runOneIterationStartTime);
  }

  private void runOneIterationForActiveProcessRunners() {
    active.forEach(a -> runOneIterationForActiveProcessRunner(a));
  }

  private void runOneIterationForActiveProcessRunner(ActiveProcessRunner activeProcessRunner) {
    synchronized (activeProcessRunner) {
      if (activeProcessRunner.getFuture() == null || activeProcessRunner.getFuture().isDone()) {
        activeProcessRunner.setFuture(
            pipeliteServices
                .executor()
                .process()
                .submit(
                    () -> {
                      // Unexpected exceptions are logged as internal errors but otherwise ignored.
                      internalErrorHandler.execute(
                          () -> {
                            ProcessRunner processRunner = activeProcessRunner.getProcessRunner();
                            Thread.currentThread()
                                .setName("pipelite-" + processRunner.getProcessId());

                            // Process runner returns true when the process execution has been
                            // completed or if an unexpected exception is thrown.
                            if (processRunner.runOneIteration()) {
                              active.remove(activeProcessRunner);
                              activeProcessRunner
                                  .getResultCallback()
                                  .accept(processRunner.getProcess());
                            }
                          });
                    }));
      }
    }
  }

  /** Returns true if the process runner is idle. */
  public boolean isIdle() {
    return active.isEmpty();
  }

  /**
   * Executes the process.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param resultCallback the process runner callback
   */
  public void runProcess(
      String pipelineName, Process process, ProcessRunnerResultCallback resultCallback) {
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessId(), "Missing process id");
    Assert.notNull(resultCallback, "Missing process runner result callback");

    // Create process runner.
    String processId = process.getProcessId();
    ProcessRunnerPool.ActiveProcessRunner activeProcessRunner =
        new ProcessRunnerPool.ActiveProcessRunner(pipelineName, processId, resultCallback);
    ProcessRunner processRunner = processRunnerFactory.create(pipelineName, process);
    activeProcessRunner.setProcessRunner(processRunner);
    active.add(activeProcessRunner);
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

  /** Terminates running processes. Process execution can't be continued later. */
  public void terminate() {
    Time.wait(processRunnerFrequency);
    active.forEach(a -> a.getProcessRunner().terminate());
  }

  /** Detaches from the running processes. Asynchronous process execution can be continued later. */
  public void detach() {
    Time.wait(processRunnerFrequency);
    active.forEach(a -> a.getProcessRunner().detach());
  }

  @Override
  public void shutDown() {
    // Guaranteed not to be called concurrently with runOneIteration.
    detach();
  }
}
