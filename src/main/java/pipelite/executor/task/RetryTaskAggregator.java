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
package pipelite.executor.task;

import lombok.extern.flogger.Flogger;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import pipelite.configuration.ServiceConfiguration;
import pipelite.exception.PipeliteTimeoutException;
import pipelite.service.InternalErrorService;
import pipelite.stage.executor.StageExecutorResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Aggregates requests and executes tasks with retries using {@link RetryTemplate}.
 *
 * @param <Request> The request type.
 * @param <ExecutorContext>> The execution context.
 */
@Flogger
public class RetryTaskAggregator<Request, ExecutorContext> {

  private final InternalErrorService internalErrorService;
  private final String serviceName;
  private final RetryTemplate retryTemplate;
  private final Map<Request, Optional<StageExecutorResult>> requests = new ConcurrentHashMap<>();
  private final int requestLimit;
  private final ExecutorContext executorContext;
  private final RetryTaskAggregatorCallback<Request, ExecutorContext> task;

  /**
   * Aggregates requests into a list, passes the list to the given task and expects the task to
   * return a map of completed requests and results. Completed requests are removed once they been
   * returned by {@link RetryTaskAggregator#getResult}.
   *
   * @param retryTemplate the retry template used in {@link RetryTaskAggregator#makeRequests}
   * @param requestLimit the maximum number of requests {@link * RetryTaskAggregator#makeRequests}
   *     will sent to the task at once
   * @param task the task that is called by {@link * RetryTaskAggregator#makeRequests}
   */
  public RetryTaskAggregator(
      ServiceConfiguration serviceConfiguration,
      InternalErrorService internalErrorService,
      RetryTemplate retryTemplate,
      int requestLimit,
      ExecutorContext executorContext,
      RetryTaskAggregatorCallback<Request, ExecutorContext> task) {
    Assert.notNull(serviceConfiguration, "Missing service configuration");
    Assert.notNull(internalErrorService, "Missing internal error service");
    this.internalErrorService = internalErrorService;
    this.serviceName = serviceConfiguration.getName();
    this.retryTemplate = retryTemplate;
    this.requestLimit = requestLimit;
    this.executorContext = executorContext;
    this.task = task;
  }

  /**
   * Aggregates requests into a list, passes the list to the given task and expects the task to
   * return a map of completed requests and results. Completed requests are removed once they been
   * returned by {@link RetryTaskAggregator#getResult}. Uses the default retry template.
   *
   * @param requestLimit the maximum number of requests {@link * RetryTaskAggregator#makeRequests}
   *     will sent to the task at once
   * @param task the task that is called by {@link * RetryTaskAggregator#makeRequests}
   */
  public RetryTaskAggregator(
      ServiceConfiguration serviceConfiguration,
      InternalErrorService internalErrorService,
      int requestLimit,
      ExecutorContext executorContext,
      RetryTaskAggregatorCallback<Request, ExecutorContext> task) {
    this(
        serviceConfiguration,
        internalErrorService,
        RetryTask.DEFAULT,
        requestLimit,
        executorContext,
        task);
  }

  protected void addRequest(Request request) {
    requests.put(request, Optional.empty());
  }

  public void makeRequests() {
    List<Request> pendingRequests = getPendingRequests();
    while (!pendingRequests.isEmpty()) {
      int toIndex = Math.min(requestLimit, pendingRequests.size());
      final List<Request> applyRequests = pendingRequests.subList(0, toIndex);
      try {
        retryTemplate.execute(
            r -> {
              Map<Request, StageExecutorResult> results =
                  task.execute(applyRequests, executorContext);
              // Set results for the requests.
              results.entrySet().stream()
                  .filter(
                      e ->
                          e.getKey() != null
                              // Filter out empty results.
                              && e.getValue() != null
                              // Filter out pending results.
                              && !e.getValue().isPending()
                              // Filter out active results.
                              && !e.getValue().isActive())
                  .forEach(e -> requests.put(e.getKey(), Optional.of(e.getValue())));
              return null;
            });
      } catch (Exception ex) {
        internalErrorService.saveInternalError(serviceName, this.getClass(), ex);
      }
      if (toIndex == pendingRequests.size()) {
        return;
      }
      pendingRequests = pendingRequests.subList(toIndex, pendingRequests.size());
    }
  }

  protected List<Request> getPendingRequests() {
    return requests.entrySet().stream()
        .filter(e -> !e.getValue().isPresent())
        .map(e -> e.getKey())
        .collect(Collectors.toList());
  }

  /**
   * Returns the result for a request once it is available. Adds a new request if one does not
   * already exist.
   *
   * @param request the request
   * @return the result
   * @throws PipeliteTimeoutException if no result has been available for a request within the
   *     request timeout
   */
  public Optional<StageExecutorResult> getResult(Request request) {
    Optional<StageExecutorResult> result = this.requests.get(request);
    if (result == null) {
      addRequest(request);
      return this.requests.get(request);
    }
    if (result.isPresent()) {
      removeRequest(request);
      return result;
    }
    return this.requests.get(request);
  }

  public void removeRequest(Request request) {
    this.requests.remove(request);
  }

  public boolean isRequest(Request request) {
    return this.requests.containsKey(request);
  }
}
