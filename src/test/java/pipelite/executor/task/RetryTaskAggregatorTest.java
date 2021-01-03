package pipelite.executor.task;

import org.junit.jupiter.api.Test;
import org.springframework.retry.support.RetryTemplate;
import pipelite.executor.task.RetryTask;
import pipelite.executor.task.RetryTaskAggregator;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class RetryTaskAggregatorTest {

  @Test
  public void test() {
    RetryTemplate retryTemplate = RetryTask.fixed(Duration.ofMillis(1), 3);
    int requestLimit = 10;
    int requestTotalCnt = 100;

    AtomicInteger taskCnt = new AtomicInteger();
    AtomicInteger requestCnt = new AtomicInteger();

    Function<List<Integer>, Map<Integer, String>> task =
        requestList -> {
          taskCnt.incrementAndGet();
          requestCnt.addAndGet(requestList.size());
          return requestList.stream().collect(Collectors.toMap(i -> i, i -> "result:" + i));
        };

    final RetryTaskAggregator aggregator =
        new RetryTaskAggregator(task, retryTemplate, requestLimit);

    IntStream.range(0, requestTotalCnt).forEach(i -> aggregator.addRequest(i));

    assertThat(aggregator.getPendingRequests(requestTotalCnt).size()).isEqualTo(requestTotalCnt);

    aggregator.makeRequests();

    assertThat(taskCnt.get()).isEqualTo(requestTotalCnt / requestLimit);
    assertThat(requestCnt.get()).isEqualTo(requestTotalCnt);

    assertThat(aggregator.getPendingRequests(requestTotalCnt).size()).isEqualTo(0);

    IntStream.range(0, requestTotalCnt)
        .forEach(
            i -> {
              assertThat(aggregator.getResult(i).get()).isEqualTo("result:" + i);
            });

    IntStream.range(0, requestTotalCnt).forEach(i -> aggregator.removeRequest(i));

    IntStream.range(0, requestTotalCnt)
        .forEach(
            i -> {
              assertThat(aggregator.getResult(i)).isNull();
            });
  }
}
