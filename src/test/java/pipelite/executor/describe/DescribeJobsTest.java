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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import pipelite.configuration.ServiceConfiguration;
import pipelite.service.InternalErrorService;
import pipelite.stage.executor.StageExecutorResult;

public class DescribeJobsTest {

  @Test
  public void success() {
    int requestLimit = 10;
    int requestCnt = 100;

    AtomicInteger actualDescribeJobsCallCnt = new AtomicInteger();
    AtomicInteger actualRequestCnt = new AtomicInteger();

    DescribeJobsCallback<Integer, String> describeJobsCallback =
        (requestList, context) -> {
          actualDescribeJobsCallCnt.incrementAndGet();
          actualRequestCnt.addAndGet(requestList.size());
          return requestList.stream()
              .collect(Collectors.toMap(i -> i, i -> StageExecutorResult.success()));
        };

    String executorContext = "test";

    final DescribeJobs<Integer, String> describeJobs =
        new DescribeJobs(
            mock(ServiceConfiguration.class),
            mock(InternalErrorService.class),
            requestLimit,
            executorContext,
            describeJobsCallback);

    IntStream.range(0, requestCnt).forEach(i -> describeJobs.addRequest(i));

    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(requestCnt);

    describeJobs.makeRequests();

    assertThat(actualDescribeJobsCallCnt.get()).isEqualTo(requestCnt / requestLimit);
    assertThat(actualRequestCnt.get()).isEqualTo(requestCnt);

    // Check that there are no active requests.
    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(0);

    // Check that the results are correct.
    IntStream.range(0, requestCnt)
        .forEach(
            i -> {
              StageExecutorResult result = describeJobs.getResult(i, null);
              assertThat(result.isSuccess()).isTrue();
            });

    // Check that the requests have been removed.
    IntStream.range(0, requestCnt).forEach(i -> assertThat(describeJobs.isRequest(i)).isFalse());
  }

  @Test
  public void error() {
    int requestLimit = 10;
    int requestCnt = 100;

    AtomicInteger actualDescribeJobsCallCnt = new AtomicInteger();
    AtomicInteger actualRequestCnt = new AtomicInteger();

    DescribeJobsCallback<Integer, String> describeJobsCallback =
        (requestList, context) -> {
          actualDescribeJobsCallCnt.incrementAndGet();
          actualRequestCnt.addAndGet(requestList.size());
          return requestList.stream()
              .collect(Collectors.toMap(i -> i, i -> StageExecutorResult.error()));
        };

    String executorContext = "test";

    final DescribeJobs<Integer, String> describeJobs =
        new DescribeJobs(
            mock(ServiceConfiguration.class),
            mock(InternalErrorService.class),
            requestLimit,
            executorContext,
            describeJobsCallback);

    IntStream.range(0, requestCnt).forEach(i -> describeJobs.addRequest(i));

    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(requestCnt);

    describeJobs.makeRequests();

    assertThat(actualDescribeJobsCallCnt.get()).isEqualTo(requestCnt / requestLimit);
    assertThat(actualRequestCnt.get()).isEqualTo(requestCnt);

    // Check that there are no active requests.
    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(0);

    // Check that the results are correct.
    IntStream.range(0, requestCnt)
        .forEach(
            i -> {
              StageExecutorResult result = describeJobs.getResult(i, null);
              assertThat(result.isError()).isTrue();
            });

    // Check that the requests have been removed.
    IntStream.range(0, requestCnt).forEach(i -> assertThat(describeJobs.isRequest(i)).isFalse());
  }

  @Test
  public void active() {
    int requestLimit = 10;
    int requestCnt = 100;

    AtomicInteger actualDescribeJobsCallCnt = new AtomicInteger();
    AtomicInteger actualRequestCnt = new AtomicInteger();

    DescribeJobsCallback<Integer, String> describeJobsCallback =
        (requestList, context) -> {
          actualDescribeJobsCallCnt.incrementAndGet();
          actualRequestCnt.addAndGet(requestList.size());
          return requestList.stream()
              .collect(Collectors.toMap(i -> i, i -> StageExecutorResult.active()));
        };

    String executorContext = "test";

    final DescribeJobs<Integer, String> describeJobs =
        new DescribeJobs(
            mock(ServiceConfiguration.class),
            mock(InternalErrorService.class),
            requestLimit,
            executorContext,
            describeJobsCallback);

    IntStream.range(0, requestCnt).forEach(i -> describeJobs.addRequest(i));

    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(requestCnt);

    describeJobs.makeRequests();

    assertThat(actualDescribeJobsCallCnt.get()).isEqualTo(requestCnt / requestLimit);
    assertThat(actualRequestCnt.get()).isEqualTo(requestCnt);

    // Check that the requests remain active.
    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(requestCnt);

    // Check that the results are correct.
    IntStream.range(0, requestCnt)
        .forEach(
            i -> {
              StageExecutorResult result = describeJobs.getResult(i, null);
              assertThat(result.isActive()).isTrue();
            });

    // Check that the requests have not been removed.
    IntStream.range(0, requestCnt).forEach(i -> assertThat(describeJobs.isRequest(i)).isTrue());
  }

  @Test
  public void exception() {
    int requestLimit = 10;
    int requestCnt = 100;

    AtomicInteger actualDescribeJobsCallCnt = new AtomicInteger();
    AtomicInteger actualRequestCnt = new AtomicInteger();

    DescribeJobsCallback<Integer, String> describeJobsCallback =
        (requestList, context) -> {
          actualDescribeJobsCallCnt.incrementAndGet();
          actualRequestCnt.addAndGet(requestList.size());
          throw new RuntimeException("Expected exception");
        };

    String executorContext = "test";

    final DescribeJobs describeJobs =
        new DescribeJobs(
            mock(ServiceConfiguration.class),
            mock(InternalErrorService.class),
            requestLimit,
            executorContext,
            describeJobsCallback);

    IntStream.range(0, requestCnt).forEach(i -> describeJobs.addRequest(i));

    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(requestCnt);

    describeJobs.makeRequests();

    // DescribeJobsCallback should be called only once with requestLimit because exception is
    // thrown.
    assertThat(actualDescribeJobsCallCnt.get()).isEqualTo(1);
    assertThat(actualRequestCnt.get()).isEqualTo(requestLimit);

    // All tasks should still be active.
    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(requestCnt);

    // Check that the requests have not been removed.
    IntStream.range(0, requestCnt).forEach(i -> assertThat(describeJobs.isRequest(i)).isTrue());
  }
}