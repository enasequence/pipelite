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
package pipelite.executor;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.request.LsfRequestContext;
import pipelite.executor.describe.poll.LsfExecutorPollJobs;
import pipelite.service.*;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=SimpleLsfExecutorPollTest",
      "pipelite.advanced.processRunnerFrequency=2s",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test"})
@DirtiesContext
public class SimpleLsfExecutorPollJobsTest {

  private static final String PIPELINE_NAME = "PIPELINE_NAME";
  private static final String PROCESS_ID = "PROCESS_ID";
  private static final String STAGE_NAME = "STAGE_NAME";

  private static final String JOB_ID = "JOB_ID";

  @SpyBean private LsfExecutorPollJobs pollJobs;
  @Autowired PipeliteServices pipeliteServices;

  private SimpleLsfExecutor mockSubmitJob() {
    SimpleLsfExecutor executor = Mockito.spy(new SimpleLsfExecutor());
    executor.setCmd("TEST");
    SimpleLsfExecutorParameters params = SimpleLsfExecutorParameters.builder().build();
    executor.setExecutorParams(params);
    Stage stage = Stage.builder().stageName(STAGE_NAME).executor(executor).build();
    executor.prepareExecution(pipeliteServices, PIPELINE_NAME, PROCESS_ID, stage);
    // Mock submit job
    when(executor.submitJob())
        .thenReturn(new AsyncExecutor.SubmitJobResult(JOB_ID, StageExecutorResult.submitted()));
    return executor;
  }

  private void mockPollJobs(Supplier<StageExecutorResult> resultCallback) {
    doAnswer(
            invocation -> {
              DescribeJobsResults<LsfRequestContext> results = new DescribeJobsResults<>();
              DescribeJobsPollRequests<LsfRequestContext> requests = invocation.getArgument(1);
              assertThat(requests.requests.size()).isOne();
              requests.requests.forEach(
                  (jobId, v) -> {
                    assertThat(jobId).isEqualTo(JOB_ID);
                    results.add(DescribeJobsResult.create(requests, jobId, resultCallback.get()));
                  });
              return results;
            })
        .when(pollJobs)
        .pollJobs(any(), any());
  }

  private StageExecutorResult execute(SimpleLsfExecutor executor) {
    while (true) {
      StageExecutorResult result = executor.execute();
      if (result.isCompleted()) {
        return result;
      }
    }
  }

  @Test
  public void testSuccess() {
    SimpleLsfExecutor executor = mockSubmitJob();
    mockPollJobs(() -> LsfExecutorPollJobs.stageExecutorResultSuccess());
    StageExecutorResult result = execute(executor);
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
  }

  @Test
  public void testError() {
    SimpleLsfExecutor executor = mockSubmitJob();
    mockPollJobs(() -> LsfExecutorPollJobs.stageExecutorResultError("1"));
    StageExecutorResult result = execute(executor);
    assertThat(result.isError()).isTrue();
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("1");
  }

  @Test
  public void testNotFound() {
    SimpleLsfExecutor executor = mockSubmitJob();
    mockPollJobs(() -> LsfExecutorPollJobs.stageExecutorResultNotFound());
    StageExecutorResult result = execute(executor);
    assertThat(result.isError()).isTrue();
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isNull();
  }
}
