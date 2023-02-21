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
package pipelite.executor.describe;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.ServiceConfiguration;
import pipelite.error.InternalErrorHandler;
import pipelite.exception.PipeliteInterruptedException;
import pipelite.exception.PipeliteTimeoutException;
import pipelite.executor.describe.context.executor.DefaultExecutorContext;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.service.InternalErrorService;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.time.Time;

/**
 * Contains active job requests and periodically retrieves job results (active, success, error). Job
 * results are retrieved * in batches to increase performance. This is called job polling. If the
 * job result can't be * retrieved then there is an attempt to recover the job result. This is
 * called job recovery.
 */
@Flogger
public class DescribeJobs<
    RequestContext extends DefaultRequestContext, ExecutorContext extends DefaultExecutorContext> {

  private static final Duration REQUEST_FREQUENCY = Duration.ofSeconds(5);

  private static final int RECOVERY_PARALLELISM = 10;
  private static final Duration RECOVERY_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration RECOVERY_WAIT = Duration.ofSeconds(5);

  private final String serviceName;
  private final Integer requestBatchSize;
  private final ExecutorContext executorContext;
  private final InternalErrorHandler internalErrorHandler;
  private final Map<RequestContext, StageExecutorResult> requests = new ConcurrentHashMap<>();
  private final Thread worker;

  public DescribeJobs(
      ServiceConfiguration serviceConfiguration,
      InternalErrorService internalErrorService,
      Integer requestBatchSize,
      ExecutorContext executorContext) {
    Assert.notNull(serviceConfiguration, "Missing service configuration");
    Assert.notNull(internalErrorService, "Missing internal error service");
    this.serviceName = serviceConfiguration.getName();
    this.requestBatchSize = requestBatchSize;
    this.executorContext = executorContext;
    this.internalErrorHandler = new InternalErrorHandler(internalErrorService, serviceName, this);
    this.worker =
        new Thread(
            () -> {
              try {
                log.atInfo().log(
                    "Starting " + executorContext.executorName() + " job result retrieval");

                AtomicBoolean interrupted = new AtomicBoolean(false);
                while (!interrupted.get()) {
                  if (Thread.interrupted()) {
                    return;
                  }
                  // Unexpected exceptions are logged as internal errors but otherwise ignored to
                  // keep describe jobs alive.
                  internalErrorHandler.execute(
                      () -> {
                        try {
                          Time.wait(REQUEST_FREQUENCY);
                        } catch (PipeliteInterruptedException ex) {
                          interrupted.set(true);
                          return;
                        }
                        retrieveResults();
                      });
                }
              } finally {
                log.atInfo().log(
                    "Shutting down " + executorContext.executorName() + " job result retrieval");
              }
            });
    worker.start();
  }

  public void shutdown() {
    worker.interrupt();
  }

  protected void addRequest(RequestContext request) {
    requests.put(request, StageExecutorResult.active());
  }

  /**
   * Called periodically to retrieve job results (active, success, error). Job results are retrieved
   * in batches to increase performance. This is called job polling. If the job result can't be
   * retrieved then there is an attempt to recover the job result. This is called job recovery.
   */
  public void retrieveResults() {
    // Unexpected exceptions are logged as internal errors but otherwise ignored to
    // keep describe jobs alive.
    internalErrorHandler.execute(
        () -> {
          List<RequestContext> activeRequests = getActiveRequests();

          log.atInfo().log(
              "Polling "
                  + activeRequests.size()
                  + " active "
                  + executorContext.executorName()
                  + " jobs");

          while (!activeRequests.isEmpty()) {
            int toIndex =
                requestBatchSize == null
                    ? activeRequests.size()
                    : Math.min(requestBatchSize, activeRequests.size());
            final List<RequestContext> requestBatch = activeRequests.subList(0, toIndex);
            Map<RequestContext, StageExecutorResult> results =
                retrieveResults(requestBatch, executorContext);
            // Set results for the requests.
            results.entrySet().stream()
                .filter(
                    // Filter out empty and active results.
                    e -> e.getKey() != null && e.getValue() != null && !e.getValue().isActive())
                .forEach(e -> this.requests.put(e.getKey(), e.getValue()));
            if (toIndex == activeRequests.size()) {
              return;
            }
            activeRequests = activeRequests.subList(toIndex, activeRequests.size());
          }
        });
  }

  /** Retrieves job results (active, success, error) for one batch. */
  private Map<RequestContext, StageExecutorResult> retrieveResults(
      List<RequestContext> requestBatch, ExecutorContext executorContext) {
    String executorName = executorContext.executorName();
    Map<RequestContext, StageExecutorResult> results = new HashMap<>();

    // Create a map for job id -> RequestContext.
    Map<String, RequestContext> requestMap = new HashMap<>();
    requestBatch.forEach(request -> requestMap.put(request.getJobId(), request));

    log.atFine().log(
        "Retrieving a batch of "
            + requestBatch.size()
            + " "
            + executorName
            + " job results "
            + requestBatch.stream().map(r -> r.getJobId()).collect(Collectors.toList()));

    // Poll jobs.
    DescribeJobsPollRequests<RequestContext> pollRequests =
        new DescribeJobsPollRequests<>(requestBatch);
    DescribeJobsResults<RequestContext> pollResults = executorContext.pollJobs(pollRequests);

    // Found results.
    pollResults.found.forEach(r -> results.put(requestMap.get(r.request.getJobId()), r.result));

    // Lost results.
    List<DescribeJobsResult<RequestContext>> lostPollResults = pollResults.lost;
    if (!lostPollResults.isEmpty()) {
      lostPollResults.forEach(
          r ->
              log.atSevere().log(
                  "Failed to retrieve result for "
                      + executorName
                      + " job "
                      + r.request.getJobId()));

      // Recover jobs.
      List<RequestContext> recoverRequests =
          lostPollResults.stream().map(r -> r.request).collect(Collectors.toList());
      DescribeJobsResults<RequestContext> recoverResults =
          recoverJobs(recoverRequests, executorContext);

      // Recovered results.
      recoverResults.found.forEach(
          r -> results.put(requestMap.get(r.request.getJobId()), r.result));

      // Lost results.
      recoverResults.lost.forEach(r -> results.put(requestMap.get(r.request.getJobId()), r.result));

      recoverResults.found.forEach(
          r ->
              log.atInfo().log(
                  "Recovered result for " + executorName + " job " + r.request.getJobId()));

      recoverResults.lost.forEach(
          r ->
              log.atSevere().log(
                  "Failed to recover result for " + executorName + " job " + r.request.getJobId()));
    }

    return results;
  }

  /**
   * Attempts to recover job results if polling has failed. Recovery is typically attempted from the
   * output file. If the recovery fails then the job is considered to have failed.
   *
   * @param requests job requests to recover.
   * @param executorContext executor context.
   * @return execution results for recovered jobs or if the recovery fails then the job is
   *     considered failed.
   */
  private DescribeJobsResults recoverJobs(
      List<RequestContext> requests, ExecutorContext executorContext) {

    DescribeJobsResults<RequestContext> results = new DescribeJobsResults<>();

    AtomicInteger remainingCount = new AtomicInteger(requests.size());
    ZonedDateTime start = ZonedDateTime.now();
    ZonedDateTime until = start.plus(RECOVERY_TIMEOUT);
    ExecutorService executorService = Executors.newFixedThreadPool(RECOVERY_PARALLELISM);
    try {
      requests.forEach(
          r ->
              executorService.submit(
                  () -> {
                    try {
                      // Attempt to recover job result.
                      DescribeJobsResult<RequestContext> recoverJobResult =
                          executorContext.recoverJob(r);
                      results.add(recoverJobResult);
                    } catch (Exception ex) {
                      log.atSevere().withCause(ex).log(
                          "Failed to recover "
                              + executorContext.executorName()
                              + " job "
                              + r.getJobId());
                    } finally {
                      remainingCount.decrementAndGet();
                    }
                  }));

      try {
        while (remainingCount.get() > 0) {
          Time.waitUntil(RECOVERY_WAIT, until);
        }
      } catch (PipeliteTimeoutException ex) {
        log.atWarning().log("Job recovery timeout exceeded.");
      }
    } finally {
      executorService.shutdownNow();
    }

    return results;
  }

  protected List<RequestContext> getActiveRequests() {
    return requests.entrySet().stream()
        .filter(e -> e.getValue().isActive())
        .map(e -> e.getKey())
        .collect(Collectors.toList());
  }

  /**
   * Returns the stage executor result (active, success, error) for the describe job request. If the
   * result is success or error then the request is removed.
   *
   * @param request the request
   * @return the result
   * @throws PipeliteTimeoutException if no result has been available for a request within the
   *     request timeout
   */
  public StageExecutorResult getResult(RequestContext request) {
    if (!this.requests.containsKey(request)) {
      addRequest(request);
    }
    StageExecutorResult result = this.requests.get(request);
    if (!result.isActive()) {
      removeRequest(request);
    }
    return result;
  }

  public void removeRequest(RequestContext request) {
    this.requests.remove(request);
  }

  public boolean isRequest(RequestContext request) {
    return this.requests.containsKey(request);
  }

  public ExecutorContext getExecutorContext() {
    return executorContext;
  }
}
