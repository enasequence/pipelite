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
import pipelite.executor.describe.DescribeJobsRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.request.LsfRequestContext;
import pipelite.stage.executor.StageExecutorResultAttribute;

public class LsfExecutorPollJobsTest {

  @Test
  public void testExtractLostJobResult() {
    DescribeJobsRequests<LsfRequestContext> requests =
        new DescribeJobsRequests<>(List.of(new LsfRequestContext("345654", "outFile")));

    assertThat(extractLostJobResult(requests, "Job <345654> is not found.").result.isLostError())
        .isTrue();
    assertThat(extractLostJobResult(requests, "Job <345654> is not found.").request.jobId())
        .isEqualTo("345654");

    assertThat(extractLostJobResult(requests, "Job <345654> is not found").result.isLostError())
        .isTrue();
    assertThat(extractLostJobResult(requests, "Job <345654> is not found.").request.jobId())
        .isEqualTo("345654");

    assertThat(extractLostJobResult(requests, "Job <345654> is ")).isNull();
    assertThat(extractLostJobResult(requests, "INVALID")).isNull();
  }

  @Test
  public void testExtractJobResultPending() {
    DescribeJobsRequests<LsfRequestContext> requests =
        new DescribeJobsRequests<>(List.of(new LsfRequestContext("861487", "outFile")));

    DescribeJobsResult<LsfRequestContext> result =
        extractJobResult(requests, "861487|PEND|-|-|-|-|-|-\n");
    assertThat(result.request.jobId()).isEqualTo("861487");
    assertThat(result.result.isActive()).isTrue();
    assertThat(result.result.attribute(StageExecutorResultAttribute.JOB_ID)).isEqualTo("861487");
  }

  @Test
  public void testExtractJobResultTimeout() {
    DescribeJobsRequests<LsfRequestContext> requests =
        new DescribeJobsRequests<>(List.of(new LsfRequestContext("861487", "outFile")));

    DescribeJobsResult<LsfRequestContext> result =
        extractJobResult(
            requests,
            "861487|EXIT|12|0.0 second(s)|-|-|hl-codon-102-04|TERM_RUNLIMIT: job killed after reaching LSF run time limit\n");
    assertThat(result.request.jobId()).isEqualTo("861487");
    assertThat(result.result.isTimeoutError()).isTrue();
    assertThat(result.result.attribute(StageExecutorResultAttribute.JOB_ID)).isEqualTo("861487");
  }

  @Test
  public void testExtractJobResultsPending() {
    DescribeJobsRequests<LsfRequestContext> requests =
        new DescribeJobsRequests<>(
            Arrays.asList(
                new LsfRequestContext("861487", "outFile"),
                new LsfRequestContext("861488", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        extractJobResults(requests, "861487|PEND|-|-|-|-|-|-\n" + "861488|PEND|-|-|-|-|-|-\n");

    DescribeJobsResult<LsfRequestContext> foundFirstResult = results.found().findFirst().get();
    DescribeJobsResult<LsfRequestContext> foundSecondResult =
        results.found().skip(1).findFirst().get();

    assertThat(results.foundCount()).isEqualTo(2);
    assertThat(foundFirstResult.request.jobId()).isEqualTo("861487");
    assertThat(foundSecondResult.request.jobId()).isEqualTo("861488");
    assertThat(foundFirstResult.result.isActive()).isTrue();
    assertThat(foundSecondResult.result.isActive()).isTrue();
    assertThat(foundFirstResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("861487");
    assertThat(foundSecondResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("861488");

    assertThat(results.lostCount()).isEqualTo(0);
  }

  @Test
  public void testExtractJobResultsDone() {
    DescribeJobsRequests<LsfRequestContext> requests =
        new DescribeJobsRequests<>(
            Arrays.asList(
                new LsfRequestContext("872793", "outFile"),
                new LsfRequestContext("872794", "outFile"),
                new LsfRequestContext("872795", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        extractJobResults(
            requests,
            "872793|DONE|-|0.0 second(s)|-|-|hx-noah-05-14|-\n"
                + "872794|DONE|-|0.0 second(s)|-|-|hx-noah-05-14|-\n"
                + "872795|DONE|-|0.0 second(s)|-|-|hx-noah-05-14|-\n");

    DescribeJobsResult<LsfRequestContext> foundFirstResult = results.found().findFirst().get();
    DescribeJobsResult<LsfRequestContext> foundSecondResult =
        results.found().skip(1).findFirst().get();
    DescribeJobsResult<LsfRequestContext> foundThirdResult =
        results.found().skip(2).findFirst().get();

    assertThat(results.foundCount()).isEqualTo(3);
    assertThat(foundFirstResult.request.jobId()).isEqualTo("872793");
    assertThat(foundSecondResult.request.jobId()).isEqualTo("872794");
    assertThat(foundThirdResult.request.jobId()).isEqualTo("872795");
    assertThat(foundFirstResult.result.isSuccess()).isTrue();
    assertThat(foundSecondResult.result.isSuccess()).isTrue();
    assertThat(foundThirdResult.result.isSuccess()).isTrue();
    assertThat(foundFirstResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("872793");
    assertThat(foundSecondResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("872794");
    assertThat(foundThirdResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("872795");

    assertThat(results.lostCount()).isEqualTo(0);
  }

  @Test
  public void testExtractJobResultsFoundAndLost() {
    DescribeJobsRequests<LsfRequestContext> requests =
        new DescribeJobsRequests<>(
            Arrays.asList(
                new LsfRequestContext("873206", "outFile"),
                new LsfRequestContext("873207", "outFile"),
                new LsfRequestContext("6065212", "outFile"),
                new LsfRequestContext("873209", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        extractJobResults(
            requests,
            "873206|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02|-\n"
                + "873207|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02|-\n"
                + "Job <6065212> is not found\n"
                + "873209|EXIT|127|0.0 second(s)|-|-|hx-noah-10-04|-\n");

    DescribeJobsResult<LsfRequestContext> foundFirstResult = results.found().findFirst().get();
    DescribeJobsResult<LsfRequestContext> foundSecondResult =
        results.found().skip(1).findFirst().get();
    DescribeJobsResult<LsfRequestContext> foundThirdResult =
        results.found().skip(2).findFirst().get();
    DescribeJobsResult<LsfRequestContext> lostFirstResult = results.lost().findFirst().get();

    assertThat(results.foundCount()).isEqualTo(3);
    assertThat(foundFirstResult.request.jobId()).isEqualTo("873206");
    assertThat(foundSecondResult.request.jobId()).isEqualTo("873207");
    assertThat(foundThirdResult.request.jobId()).isEqualTo("873209");
    assertThat(foundFirstResult.result.isExecutionError()).isTrue();
    assertThat(foundSecondResult.result.isExecutionError()).isTrue();
    assertThat(foundThirdResult.result.isExecutionError()).isTrue();
    assertThat(foundFirstResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("873206");
    assertThat(foundSecondResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("873207");
    assertThat(foundThirdResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("873209");

    assertThat(results.lostCount()).isEqualTo(1);
    assertThat(lostFirstResult.request.jobId()).isEqualTo("6065212");
    assertThat(lostFirstResult.result.isLostError()).isTrue();
    assertThat(lostFirstResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("6065212");
  }

  @Test
  public void testExtractJobResultsLost() {
    DescribeJobsRequests<LsfRequestContext> requests =
        new DescribeJobsRequests<>(
            Arrays.asList(
                new LsfRequestContext("333", "outFile"), new LsfRequestContext("222", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        extractJobResults(requests, "Job <333> is not found\n" + "Job <222> is not found\n");

    DescribeJobsResult<LsfRequestContext> lostFirstResult = results.lost().findFirst().get();
    DescribeJobsResult<LsfRequestContext> lostSecondResult =
        results.lost().skip(1).findFirst().get();

    assertThat(results.foundCount()).isEqualTo(0);
    assertThat(results.lostCount()).isEqualTo(2);

    assertThat(lostFirstResult.request.jobId()).isEqualTo("333");
    assertThat(lostFirstResult.result.isLostError()).isTrue();
    assertThat(lostFirstResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("333");

    assertThat(lostSecondResult.request.jobId()).isEqualTo("222");
    assertThat(lostSecondResult.result.isLostError()).isTrue();
    assertThat(lostSecondResult.result.attribute(StageExecutorResultAttribute.JOB_ID))
        .isEqualTo("222");
  }
}
