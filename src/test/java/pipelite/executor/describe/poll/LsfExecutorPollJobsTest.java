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
package pipelite.executor.describe.poll;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.executor.describe.poll.LsfExecutorPollJobs.*;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.request.LsfRequestContext;

public class LsfExecutorPollJobsTest {

  @Test
  public void testExtractNotFoundJobIdFromPollOutput() {
    assertThat(extractNotFoundJobIdFromPollOutput("Job <345654> is not found."))
        .isEqualTo("345654");
    assertThat(extractNotFoundJobIdFromPollOutput("Job <345654> is not found")).isEqualTo("345654");
    assertThat(extractNotFoundJobIdFromPollOutput("Job <345654> is ")).isNull();
    assertThat(extractNotFoundJobIdFromPollOutput("INVALID")).isNull();
  }

  @Test
  public void testExtractJobResultFromPollOutputPending() {
    LsfRequestContext requestContext = new LsfRequestContext("861487", "outFile");
    DescribeJobsPollRequests<LsfRequestContext> requests =
        new DescribeJobsPollRequests<>(List.of(requestContext));
    DescribeJobsResult<LsfRequestContext> result =
        extractJobResultFromPollOutput("861487|PEND|-|-|-|-|-\n", requests);
    assertThat(result.request.getJobId()).isEqualTo("861487");
    assertThat(result.result.isActive()).isTrue();
  }

  @Test
  public void testExtractResultsFromPollOutputPending() {
    DescribeJobsPollRequests<LsfRequestContext> requests =
        new DescribeJobsPollRequests<>(
            Arrays.asList(
                new LsfRequestContext("861487", "outFile"),
                new LsfRequestContext("861488", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        extractJobResultsFromPollOutput(
            "861487|PEND|-|-|-|-|-\n" + "861488|PEND|-|-|-|-|-\n", requests);

    assertThat(results.found.size()).isEqualTo(2);
    assertThat(results.notFound.size()).isEqualTo(0);
    assertThat(results.found.get(0).request.getJobId()).isEqualTo("861487");
    assertThat(results.found.get(1).request.getJobId()).isEqualTo("861488");
    assertThat(results.found.get(0).result.isActive()).isTrue();
    assertThat(results.found.get(1).result.isActive()).isTrue();
  }

  @Test
  public void testExtractResultsFromPollOutputDone() {
    DescribeJobsPollRequests<LsfRequestContext> requests =
        new DescribeJobsPollRequests<>(
            Arrays.asList(
                new LsfRequestContext("872793", "outFile"),
                new LsfRequestContext("872794", "outFile"),
                new LsfRequestContext("872795", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        extractJobResultsFromPollOutput(
            "872793|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n"
                + "872794|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n"
                + "872795|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n",
            requests);

    assertThat(results.found.size()).isEqualTo(3);
    assertThat(results.found.get(0).request.getJobId()).isEqualTo("872793");
    assertThat(results.found.get(1).request.getJobId()).isEqualTo("872794");
    assertThat(results.found.get(2).request.getJobId()).isEqualTo("872795");
    assertThat(results.found.get(0).result.isSuccess()).isTrue();
    assertThat(results.found.get(1).result.isSuccess()).isTrue();
    assertThat(results.found.get(2).result.isSuccess()).isTrue();
  }

  @Test
  public void testExtractResultsFromPollExitAndNotFound() {
    DescribeJobsPollRequests<LsfRequestContext> requests =
        new DescribeJobsPollRequests<>(
            Arrays.asList(
                new LsfRequestContext("873206", "outFile"),
                new LsfRequestContext("873207", "outFile"),
                new LsfRequestContext("6065212", "outFile"),
                new LsfRequestContext("873209", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        extractJobResultsFromPollOutput(
            "873206|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02\n"
                + "873207|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02\n"
                + "Job <6065212> is not found\n"
                + "873209|EXIT|127|0.0 second(s)|-|-|hx-noah-10-04\n",
            requests);

    assertThat(results.found.size()).isEqualTo(3);
    assertThat(results.notFound.size()).isEqualTo(1);
    assertThat(results.found.get(0).request.getJobId()).isEqualTo("873206");
    assertThat(results.found.get(1).request.getJobId()).isEqualTo("873207");
    assertThat(results.notFound.get(0).getJobId()).isEqualTo("6065212");
    assertThat(results.found.get(2).request.getJobId()).isEqualTo("873209");
    assertThat(results.found.get(0).result.isError()).isTrue();
    assertThat(results.found.get(1).result.isError()).isTrue();
    assertThat(results.found.get(2).result.isError()).isTrue();
  }
}
