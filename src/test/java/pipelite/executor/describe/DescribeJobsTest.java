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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.describe.context.executor.DefaultExecutorContext;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.executor.describe.poll.PollJobs;
import pipelite.executor.describe.recover.RecoverJob;
import pipelite.service.InternalErrorService;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.time.Time;

public class DescribeJobsTest {

  private static class TestExecutorContext extends DefaultExecutorContext<DefaultRequestContext> {
    private final PollJobs<DefaultExecutorContext<DefaultRequestContext>, DefaultRequestContext>
        pollJobs;
    private final RecoverJob<DefaultExecutorContext<DefaultRequestContext>, DefaultRequestContext>
        recoverJob;

    public TestExecutorContext(
        PollJobs<DefaultExecutorContext<DefaultRequestContext>, DefaultRequestContext> pollJobs,
        RecoverJob<DefaultExecutorContext<DefaultRequestContext>, DefaultRequestContext>
            recoverJob) {
      super("TestExecutor");
      this.pollJobs = pollJobs;
      this.recoverJob = recoverJob;
    }

    @Override
    public DescribeJobsResults<DefaultRequestContext> pollJobs(
        DescribeJobsRequests<DefaultRequestContext> requests) {
      return pollJobs.pollJobs(this, requests);
    }

    @Override
    public DescribeJobsResult<DefaultRequestContext> recoverJob(DefaultRequestContext request) {
      return recoverJob.recoverJob(this, request);
    }
  }

  private static DefaultRequestContext requestContext(int jobId) {
    return new DefaultRequestContext(String.valueOf(jobId));
  }

  private static void testDescribeJobsWithMockPollJobs(StageExecutorResult result) {
    int requestLimit = 10;
    int requestCnt = 20;

    AtomicInteger actualDescribeJobsCallCnt = new AtomicInteger();
    AtomicInteger actualRequestCnt = new AtomicInteger();

    PollJobs<DefaultExecutorContext<DefaultRequestContext>, DefaultRequestContext> pollJobs =
        (executorContext, requests) -> {
          DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
          actualDescribeJobsCallCnt.incrementAndGet();
          actualRequestCnt.addAndGet(requests.size());
          requests
              .get()
              .forEach(
                  request ->
                      results.add(DescribeJobsResult.builder(request).result(result).build()));
          return results;
        };

    RecoverJob<DefaultExecutorContext<DefaultRequestContext>, DefaultRequestContext> recoverJob =
        (executorContext, request) -> DescribeJobsResult.builder(request).lostError().build();

    TestExecutorContext executorContext = new TestExecutorContext(pollJobs, recoverJob);

    try (final DescribeJobs<DefaultRequestContext, TestExecutorContext> describeJobs =
        new DescribeJobs<>(
            mock(ServiceConfiguration.class),
            mock(InternalErrorService.class),
            requestLimit,
            executorContext)) {

      IntStream.range(0, requestCnt).forEach(i -> describeJobs.addRequest(requestContext(i)));
      assertThat(describeJobs.getActiveRequests().size()).isEqualTo(requestCnt);

      // DescribeJobs has a worker thread that calls retrieveResults.
      AtomicInteger retrieveResultsCount = new AtomicInteger();
      describeJobs.setRetrieveResultsListener(e -> retrieveResultsCount.incrementAndGet());
      while (retrieveResultsCount.get() == 0) {
        Time.wait(Duration.ofSeconds(1));
      }

      assertThat(actualDescribeJobsCallCnt.get()).isEqualTo(requestCnt / requestLimit);
      assertThat(actualRequestCnt.get()).isEqualTo(requestCnt);

      // Check that the results are correct.
      IntStream.range(0, requestCnt)
          .forEach(
              i -> {
                StageExecutorResult actualResult = describeJobs.getResult(requestContext(i));
                assertThat(actualResult.state()).isEqualTo(result.state());
              });

      if (result.isCompleted()) {
        // Check that the requests have been removed.
        IntStream.range(0, requestCnt)
            .forEach(i -> assertThat(describeJobs.isRequest(requestContext(i))).isFalse());
      } else {
        // Check that the requests have not been removed.
        IntStream.range(0, requestCnt)
            .forEach(i -> assertThat(describeJobs.isRequest(requestContext(i))).isTrue());
      }
    }
  }

  @Test
  public void testDescribeJobsWithMockPollJobs() {
    testDescribeJobsWithMockPollJobs(StageExecutorResult.success());
    testDescribeJobsWithMockPollJobs(StageExecutorResult.executionError());
    testDescribeJobsWithMockPollJobs(StageExecutorResult.timeoutError());
    testDescribeJobsWithMockPollJobs(StageExecutorResult.internalError());
    testDescribeJobsWithMockPollJobs(StageExecutorResult.lostError());
    testDescribeJobsWithMockPollJobs(StageExecutorResult.active());
  }

  @Test
  public void testDescribeJobsWithMockPollJobsException() {
    int requestLimit = 10;
    int requestCnt = 20;

    AtomicInteger actualDescribeJobsCallCnt = new AtomicInteger();
    AtomicInteger actualRequestCnt = new AtomicInteger();

    PollJobs<DefaultExecutorContext<DefaultRequestContext>, DefaultRequestContext> pollJobs =
        (executorContext, requests) -> {
          actualDescribeJobsCallCnt.incrementAndGet();
          actualRequestCnt.addAndGet(requests.size());
          throw new RuntimeException("Expected exception");
        };

    TestExecutorContext executorContext = new TestExecutorContext(pollJobs, null);

    final DescribeJobs<DefaultRequestContext, TestExecutorContext> describeJobs =
        new DescribeJobs<>(
            mock(ServiceConfiguration.class),
            mock(InternalErrorService.class),
            requestLimit,
            executorContext);

    IntStream.range(0, requestCnt).forEach(i -> describeJobs.addRequest(requestContext(i)));

    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(requestCnt);

    // DescribeJobs has a worker thread that calls retrieveResults.
    AtomicInteger retrieveResultsCount = new AtomicInteger();
    describeJobs.setRetrieveResultsListener(e -> retrieveResultsCount.incrementAndGet());
    while (retrieveResultsCount.get() == 0) {
      Time.wait(Duration.ofSeconds(1));
    }

    // DescribeJobsCallback should be called only once with requestLimit because exception is
    // thrown.
    assertThat(actualDescribeJobsCallCnt.get()).isEqualTo(1);
    assertThat(actualRequestCnt.get()).isEqualTo(requestLimit);

    // All tasks should still be active.
    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(requestCnt);

    // Check that the requests have not been removed.
    IntStream.range(0, requestCnt)
        .forEach(i -> assertThat(describeJobs.isRequest(requestContext(i))).isTrue());
  }

  @Test
  public void testRecoverJobSuccess() {
    DefaultRequestContext request = new DefaultRequestContext("anyJobId");
    DefaultExecutorContext executorContext = mock(DefaultExecutorContext.class);

    when(executorContext.recoverJob(any()))
        .thenReturn(DescribeJobsResult.builder(request).success().build());
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    DescribeJobs.recoverJob(executorContext, request, results);
    assertThat(results.lostCount()).isEqualTo(0);
    assertThat(results.foundCount()).isEqualTo(1);
    DescribeJobsResult<DefaultRequestContext> foundFirstResult = results.found().findFirst().get();
    assertThat(foundFirstResult.jobId()).isEqualTo(request.jobId());
    assertThat(foundFirstResult.result.isSuccess()).isTrue();
  }

  @Test
  public void testRecoverJobTimeoutError() {
    DefaultRequestContext request = new DefaultRequestContext("anyJobId");
    DefaultExecutorContext executorContext = mock(DefaultExecutorContext.class);

    when(executorContext.recoverJob(any()))
        .thenReturn(DescribeJobsResult.builder(request).timeoutError().build());
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    DescribeJobs.recoverJob(executorContext, request, results);
    assertThat(results.lostCount()).isEqualTo(0);
    assertThat(results.foundCount()).isEqualTo(1);
    DescribeJobsResult<DefaultRequestContext> foundFirstResult = results.found().findFirst().get();
    assertThat(foundFirstResult.jobId()).isEqualTo(request.jobId());
    assertThat(foundFirstResult.result.isTimeoutError()).isTrue();
    assertThat(foundFirstResult.result.isError()).isTrue();
  }

  @Test
  public void testRecoverJobExecutionErrorWithExitCode() {
    DefaultRequestContext request = new DefaultRequestContext("anyJobId");
    DefaultExecutorContext executorContext = mock(DefaultExecutorContext.class);

    when(executorContext.recoverJob(any()))
        .thenReturn(DescribeJobsResult.builder(request).executionError(1).build());
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    DescribeJobs.recoverJob(executorContext, request, results);
    assertThat(results.lostCount()).isEqualTo(0);
    assertThat(results.foundCount()).isEqualTo(1);
    DescribeJobsResult<DefaultRequestContext> foundFirstResult = results.found().findFirst().get();
    assertThat(foundFirstResult.jobId()).isEqualTo(request.jobId());
    assertThat(foundFirstResult.result.isExecutionError()).isTrue();
    assertThat(foundFirstResult.result.isError()).isTrue();
    assertThat(
            results
                .found()
                .findFirst()
                .get()
                .result
                .attribute(StageExecutorResultAttribute.EXIT_CODE))
        .isEqualTo("1");
  }

  @Test
  public void testRecoverJobExecutionErrorWithoutExitCode() {
    DefaultRequestContext request = new DefaultRequestContext("anyJobId");
    DefaultExecutorContext executorContext = mock(DefaultExecutorContext.class);

    when(executorContext.recoverJob(any()))
        .thenReturn(DescribeJobsResult.builder(request).executionError().build());
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    DescribeJobs.recoverJob(executorContext, request, results);
    assertThat(results.lostCount()).isEqualTo(0);
    assertThat(results.foundCount()).isEqualTo(1);
    DescribeJobsResult<DefaultRequestContext> foundFirstResult = results.found().findFirst().get();
    assertThat(foundFirstResult.jobId()).isEqualTo(request.jobId());
    assertThat(foundFirstResult.result.isExecutionError()).isTrue();
    assertThat(foundFirstResult.result.isError()).isTrue();
  }

  @Test
  public void testRecoverJobLostError() {
    DefaultRequestContext request = new DefaultRequestContext("anyJobId");
    DefaultExecutorContext executorContext = mock(DefaultExecutorContext.class);

    // Lost error
    when(executorContext.recoverJob(any()))
        .thenReturn(DescribeJobsResult.builder(request).lostError().build());
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    DescribeJobs.recoverJob(executorContext, request, results);
    assertThat(results.lostCount()).isEqualTo(1);
    DescribeJobsResult<DefaultRequestContext> lostFirstResult = results.lost().findFirst().get();
    assertThat(lostFirstResult.jobId()).isEqualTo(request.jobId());
    assertThat(lostFirstResult.result.isLostError()).isTrue();
    assertThat(lostFirstResult.result.isError()).isTrue();
    assertThat(results.foundCount()).isEqualTo(0);
  }

  @Test
  public void testRecoverJobLostErrorException() {
    DefaultRequestContext request = new DefaultRequestContext("anyJobId");
    DefaultExecutorContext executorContext = mock(DefaultExecutorContext.class);

    when(executorContext.recoverJob(any())).thenThrow(new RuntimeException("any"));
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    DescribeJobs.recoverJob(executorContext, request, results);
    assertThat(results.lostCount()).isEqualTo(1);
    DescribeJobsResult<DefaultRequestContext> lostFirstResult = results.lost().findFirst().get();
    assertThat(lostFirstResult.jobId()).isEqualTo(request.jobId());
    assertThat(lostFirstResult.result.isLostError()).isTrue();
    assertThat(lostFirstResult.result.isError()).isTrue();
    assertThat(results.foundCount()).isEqualTo(0);
  }

  @Test
  public void testRecoverJobLostErrorNull() {
    DefaultRequestContext request = new DefaultRequestContext("anyJobId");
    DefaultExecutorContext executorContext = mock(DefaultExecutorContext.class);

    doReturn(null).when(executorContext).recoverJob(any());
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    DescribeJobs.recoverJob(executorContext, request, results);
    assertThat(results.lostCount()).isEqualTo(1);
    DescribeJobsResult<DefaultRequestContext> lostFirstResult = results.lost().findFirst().get();
    assertThat(lostFirstResult.jobId()).isEqualTo(request.jobId());
    assertThat(lostFirstResult.result.isLostError()).isTrue();
    assertThat(lostFirstResult.result.isError()).isTrue();
    assertThat(results.foundCount()).isEqualTo(0);
  }
}
