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
package pipelite.executor.describe;

import com.google.common.primitives.Ints;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.ServiceConfiguration;
import pipelite.error.InternalErrorHandler;
import pipelite.exception.PipeliteException;
import pipelite.exception.PipeliteTimeoutException;
import pipelite.service.InternalErrorService;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;

/**
 * Aggregates and executes describe job requests and returns the results (active, success, error).
 * If the result is success or error then the request is removed.
 *
 * @param <RequestContext> The request context.
 * @param <ExecutorContext>> The executor context.
 */
@Flogger
public class DescribeJobs<RequestContext, ExecutorContext> {

  private final String serviceName;
  private final Integer requestLimit;
  private final ExecutorContext executorContext;
  private final DescribeJobsCallback<RequestContext, ExecutorContext> describeJobsCallback;
  private final InternalErrorHandler internalErrorHandler;
  private final Map<RequestContext, StageExecutorResult> requests = new ConcurrentHashMap<>();

  public DescribeJobs(
      ServiceConfiguration serviceConfiguration,
      InternalErrorService internalErrorService,
      Integer requestLimit,
      ExecutorContext executorContext,
      DescribeJobsCallback<RequestContext, ExecutorContext> describeJobsCallback) {
    Assert.notNull(serviceConfiguration, "Missing service configuration");
    Assert.notNull(internalErrorService, "Missing internal error service");
    this.serviceName = serviceConfiguration.getName();
    this.requestLimit = requestLimit;
    this.executorContext = executorContext;
    this.describeJobsCallback = describeJobsCallback;
    this.internalErrorHandler = new InternalErrorHandler(internalErrorService, serviceName, this);
  }

  protected void addRequest(RequestContext request) {
    requests.put(request, StageExecutorResult.active());
  }

  public void makeRequests() {
    internalErrorHandler.execute(
        () -> {
          List<RequestContext> activeRequests = getActiveRequests();
          while (!activeRequests.isEmpty()) {
            int toIndex =
                requestLimit == null
                    ? activeRequests.size()
                    : Math.min(requestLimit, activeRequests.size());
            final List<RequestContext> activeSubRequests = activeRequests.subList(0, toIndex);
            Map<RequestContext, StageExecutorResult> results =
                describeJobsCallback.execute(activeSubRequests, executorContext);
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
  public StageExecutorResult getResult(RequestContext request, List<Integer> permanentErrors) {
    if (!this.requests.containsKey(request)) {
      addRequest(request);
    }
    StageExecutorResult result = this.requests.get(request);
    if (!result.isActive()) {
      removeRequest(request);
      setPermanentError(result, permanentErrors);
    }
    return result;
  }

  static void setPermanentError(StageExecutorResult result, List<Integer> permanentErrors) {
    if (permanentErrors == null) {
      return;
    }
    String exitCode = result.getAttribute(StageExecutorResultAttribute.EXIT_CODE);
    if (exitCode == null || Ints.tryParse(exitCode) == null) {
      throw new PipeliteException("Missing or invalid exit code: " + exitCode);
    }
    if (permanentErrors.contains(Ints.tryParse(exitCode))) {
      result.setPermanentError();
    }
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
