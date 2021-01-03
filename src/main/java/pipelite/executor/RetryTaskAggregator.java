package pipelite.executor;

import com.google.common.collect.Maps;
import org.springframework.retry.support.RetryTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Aggregates requests and calls a task to return results.
 *
 * @param <Request> The request type.
 * @param <Result> The result type.
 */
public class RetryTaskAggregator<Request, Result> {

  private final Function<List<Request>, Map<Request, Result>> task;
  private final RetryTemplate retryTemplate;
  private final Map<Request, Optional<Result>> requests = new ConcurrentHashMap<>();
  private final int requestLimit;

  /**
   * Aggregates requests and calls the task to return results.
   *
   * @param task the task that will be given a list of requests to return as a map of requests
   *     mapped to results.
   * @param retryTemplate the retry template used to call the task
   */
  public RetryTaskAggregator(
      Function<List<Request>, Map<Request, Result>> task,
      RetryTemplate retryTemplate,
      int requestLimit) {
    this.task = task;
    this.retryTemplate = retryTemplate;
    this.requestLimit = requestLimit;
  }

  public void addRequest(Request request) {
    requests.put(request, Optional.empty());
  }

  public void makeRequests() {
    List<Request> pendingRequests = getPendingRequests(requestLimit);
    while (!pendingRequests.isEmpty()) {
      final List<Request> taskRequests = pendingRequests;
      retryTemplate.execute(
          r -> {
            Map<Request, Result> results = task.apply(taskRequests);
            results.entrySet().stream()
                .filter(e -> e.getKey() != null && e.getValue() != null)
                .forEach(e -> requests.put(e.getKey(), Optional.of(e.getValue())));
            return null;
          });
      pendingRequests = getPendingRequests(requestLimit);
    }
  }

  public List<Request> getPendingRequests(int requestLimit) {
    return requests.entrySet().stream()
        .filter(e -> !e.getValue().isPresent())
        .map(e -> e.getKey())
        .limit(requestLimit)
        .collect(Collectors.toList());
  }

  public Optional<Result> getResult(Request request) {
    return this.requests.get(request);
  }

  public void removeRequest(Request request) {
    this.requests.remove(request);
  }
}
