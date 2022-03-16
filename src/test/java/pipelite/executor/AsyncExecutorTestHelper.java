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
package pipelite.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import pipelite.UniqueStringGenerator;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.DescribeJobsCacheService;
import pipelite.service.PipeliteExecutorService;
import pipelite.stage.Stage;
import pipelite.stage.executor.ErrorType;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultCallback;
import pipelite.time.Time;

public class AsyncExecutorTestHelper {

  public static void testExecute(
      AbstractAsyncExecutor<?, ?> executor,
      PipeliteExecutorService pipeliteExecutorService,
      DescribeJobsCacheService describeJobsCacheService,
      PipeliteMetrics pipeliteMetrics,
      StageExecutorResultCallback assertAfterSubmit,
      StageExecutorResultCallback assertAfterPoll) {

    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    String stageName = UniqueStringGenerator.randomStageName();
    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName(pipelineName)
            .processId(processId)
            .stage(stage)
            .build();

    executor.setSubmitExecutorService(pipeliteExecutorService.submitStage());
    executor.setDescribeJobsService(describeJobsCacheService);
    executor.setStageMetrics(pipeliteMetrics.pipeline(pipelineName).stage());

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();

    executor.execute(request, (r) -> result.set(r));

    while (result.get() == null) {
      Time.wait(Duration.ofSeconds(1));
    }

    assertThat(result.get().isSubmitted()).isTrue();
    assertAfterSubmit.accept(result.get());

    while (!result.get().isSuccess() && !result.get().isError()) {
      executor.execute(request, (r) -> result.set(r));
      Time.wait(Duration.ofSeconds(1));
    }

    // Ignore timeout errors.
    if (result.get().isErrorType(ErrorType.TIMEOUT_ERROR)) {
      return;
    }

    assertAfterPoll.accept(result.get());
  }
}
