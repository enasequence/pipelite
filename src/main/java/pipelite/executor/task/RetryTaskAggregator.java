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

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.retry.support.RetryTemplate;
import pipelite.exception.PipeliteTimeoutException;

/**
 * Aggregates requests and executes tasks with retries using {@link RetryTemplate}.
 *
 * @param <Request> The request type.
 * @param <Result> The result type.
 * @param <ExecutorContext>> The execution context.
 */
@Flogger
public class RetryTaskAggregator<Request, Result, ExecutorContext> {

  private final RetryTemplate retryTemplate;
  private final Map<Request, Optional<Result>> requests = new ConcurrentHashMap<>();
  private final Map<Request, ZonedDateTime> requestTimes = new ConcurrentHashMap<>();
  private final Duration requestTimeout;
  private final int requestLimit;
  private final ExecutorContext executorContext;
  private final RetryTaskAggregatorCallback<Request, Result, ExecutorContext> task;

  /**
   * Aggregates requests into a list, passes the list to the given task and expects the task to
   * return a map of completed requests and results. Completed requests are removed once they been
   * returned by {@link RetryTaskAggregator#getResult}.
   *
   * @param retryTemplate the retry template used in {@link RetryTaskAggregator#makeRequests}
   * @param requestTimeout the timeout for a task being executed by {@link
   *     RetryTaskAggregator#makeRequests}
   * @param requestLimit the maximum number of requests {@link * RetryTaskAggregator#makeRequests}
   *     will sent to the task at once
   * @param task the task that is called by {@link * RetryTaskAggregator#makeRequests}
   */
  public RetryTaskAggregator(
      RetryTemplate retryTemplate,
      Duration requestTimeout,
      int requestLimit,
      ExecutorContext executorContext,
      RetryTaskAggregatorCallback<Request, Result, ExecutorContext> task) {
    this.executorContext = executorContext;
    this.task = task;
    this.retryTemplate = retryTemplate;
    this.requestTimeout = requestTimeout;
    this.requestLimit = requestLimit;
  }

  protected void addRequest(Request request) {
    requests.put(request, Optional.empty());
    requestTimes.put(request, ZonedDateTime.now());
  }

  public void makeRequests() {
    List<Request> pendingRequests = getPendingRequests();
    while (!pendingRequests.isEmpty()) {
      int toIndex = Math.min(requestLimit, pendingRequests.size());
      final List<Request> applyRequests = pendingRequests.subList(0, toIndex);
      try {
        retryTemplate.execute(
            r -> {
              Map<Request, Result> results = task.execute(applyRequests, executorContext);
              // Set results for the requests.
              results.entrySet().stream()
                  .filter(e -> e.getKey() != null && e.getValue() != null)
                  .forEach(e -> requests.put(e.getKey(), Optional.of(e.getValue())));
              return null;
            });
      } catch (Exception ex) {
        log.atSevere().withCause(ex).log("Unexpected exception in RetryTaskAggregator");
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
  public Optional<Result> getResult(Request request) {
    Optional<Result> result = this.requests.get(request);
    if (result == null) {
      addRequest(request);
      return this.requests.get(request);
    }
    if (result.isPresent()) {
      removeRequest(request);
      return result;
    }
    if (isTimeout(request)) {
      removeRequest(request);
      throw new PipeliteTimeoutException("Task timeout");
    }
    return this.requests.get(request);
  }

  /**
   * Returns true if the request has timed out.
   *
   * @param request the request
   * @return true if the request has timed out.
   */
  public boolean isTimeout(Request request) {
    ZonedDateTime requestTime = this.requestTimes.get(request);
    if (requestTime != null) {
      return requestTime.plus(requestTimeout).isBefore(ZonedDateTime.now());
    }
    return false;
  }

  protected void removeRequest(Request request) {
    this.requests.remove(request);
    this.requestTimes.remove(request);
  }

  public boolean isRequest(Request request) {
    return this.requests.containsKey(request);
  }
}
