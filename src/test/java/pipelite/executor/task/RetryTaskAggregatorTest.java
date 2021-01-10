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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.retry.support.RetryTemplate;
import pipelite.exception.PipeliteTimeoutException;
import pipelite.time.Time;

public class RetryTaskAggregatorTest {

  @Test
  public void success() {
    RetryTemplate retryTemplate = RetryTask.fixed(Duration.ofMillis(1), 3);
    Duration requestTimeout = Duration.ofSeconds(10);
    int requestLimit = 10;
    int requestTotalCnt = 100;

    AtomicInteger taskCnt = new AtomicInteger();
    AtomicInteger requestCnt = new AtomicInteger();

    RetryTaskAggregatorCallback<Integer, String, String> task =
        (requestList, context) -> {
          taskCnt.incrementAndGet();
          requestCnt.addAndGet(requestList.size());
          return requestList.stream().collect(Collectors.toMap(i -> i, i -> "result:" + i));
        };

    String context = "test";

    final RetryTaskAggregator aggregator =
        new RetryTaskAggregator(retryTemplate, requestTimeout, requestLimit, context, task);

    IntStream.range(0, requestTotalCnt).forEach(i -> aggregator.addRequest(i));

    assertThat(aggregator.getPendingRequests().size()).isEqualTo(requestTotalCnt);

    aggregator.makeRequests();

    assertThat(taskCnt.get()).isEqualTo(requestTotalCnt / requestLimit);
    assertThat(requestCnt.get()).isEqualTo(requestTotalCnt);

    // Check that there are no pending requests.
    assertThat(aggregator.getPendingRequests().size()).isEqualTo(0);

    // Check that the results are correct.
    IntStream.range(0, requestTotalCnt)
        .forEach(i -> assertThat(aggregator.getResult(i).get()).isEqualTo("result:" + i));

    // IntStream.range(0, requestTotalCnt).forEach(i -> aggregator.removeRequest(i));

    // Check that the requests have been removed.
    IntStream.range(0, requestTotalCnt).forEach(i -> assertThat(aggregator.isRequest(i)).isFalse());
  }

  @Test
  public void errorWithTimeout() {
    RetryTemplate retryTemplate = RetryTask.fixed(Duration.ofMillis(1), 1);
    Duration requestTimeout = Duration.ofSeconds(1);
    int requestLimit = 10;
    int requestTotalCnt = 100;

    AtomicInteger taskCnt = new AtomicInteger();
    AtomicInteger requestCnt = new AtomicInteger();

    RetryTaskAggregatorCallback<Integer, String, String> task =
        (requestList, context) -> {
          taskCnt.incrementAndGet();
          requestCnt.addAndGet(requestList.size());
          throw new RuntimeException("Expected exception");
        };

    String context = "test";

    final RetryTaskAggregator aggregator =
        new RetryTaskAggregator(retryTemplate, requestTimeout, requestLimit, context, task);

    IntStream.range(0, requestTotalCnt).forEach(i -> aggregator.addRequest(i));

    assertThat(aggregator.getPendingRequests().size()).isEqualTo(requestTotalCnt);

    aggregator.makeRequests();

    assertThat(taskCnt.get()).isEqualTo(requestTotalCnt / requestLimit);
    assertThat(requestCnt.get()).isEqualTo(requestTotalCnt);

    // All tasks should still be pending.
    assertThat(aggregator.getPendingRequests().size()).isEqualTo(requestTotalCnt);

    Time.wait(Duration.ofSeconds(2));

    // All tasks should still be pending.
    assertThat(aggregator.getPendingRequests().size()).isEqualTo(requestTotalCnt);

    // Timeout does not affect makeRequests.
    aggregator.makeRequests();

    assertThat(taskCnt.get()).isEqualTo(2 * requestTotalCnt / requestLimit);
    assertThat(requestCnt.get()).isEqualTo(2 * requestTotalCnt);

    // Timeout does not affect pending requests.
    assertThat(aggregator.getPendingRequests().size()).isEqualTo(requestTotalCnt);

    // Check that the results throw a timeout exception.
    IntStream.range(0, requestTotalCnt)
        .forEach(
            i -> assertThrows(PipeliteTimeoutException.class, () -> aggregator.getResult(i).get()));

    // Check that the pending queue is now empty.
    assertThat(aggregator.getPendingRequests().size()).isZero();

    // Check that the requests have been removed.
    IntStream.range(0, requestTotalCnt).forEach(i -> assertThat(aggregator.isRequest(i)).isFalse());
  }

  @Test
  public void errorWithRetry() {
    RetryTemplate retryTemplate = RetryTask.fixed(Duration.ofMillis(1), 3);
    Duration requestTimeout = Duration.ofSeconds(1);
    int requestLimit = 10;
    int requestTotalCnt = 100;

    AtomicInteger taskCnt = new AtomicInteger();
    AtomicInteger requestCnt = new AtomicInteger();

    RetryTaskAggregatorCallback<Integer, String, String> task =
        (requestList, context) -> {
          taskCnt.incrementAndGet();
          requestCnt.addAndGet(requestList.size());
          throw new RuntimeException("Expected exception");
        };

    String context = "test";

    final RetryTaskAggregator aggregator =
        new RetryTaskAggregator(retryTemplate, requestTimeout, requestLimit, context, task);

    IntStream.range(0, requestTotalCnt).forEach(i -> aggregator.addRequest(i));

    assertThat(aggregator.getPendingRequests().size()).isEqualTo(requestTotalCnt);

    aggregator.makeRequests();

    assertThat(taskCnt.get()).isEqualTo(3 * requestTotalCnt / requestLimit);
    assertThat(requestCnt.get()).isEqualTo(3 * requestTotalCnt);

    // All tasks should still be pending.
    assertThat(aggregator.getPendingRequests().size()).isEqualTo(requestTotalCnt);
  }
}
