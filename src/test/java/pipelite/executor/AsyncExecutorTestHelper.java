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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.function.Consumer;
import pipelite.PipeliteIdCreator;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.time.Time;

public class AsyncExecutorTestHelper {

  @FunctionalInterface
  public interface StageExecutorResultCallback extends Consumer<StageExecutorResult> {}

  public static void testExecute(
      AsyncExecutor<?, ?, ?> executor,
      PipeliteServices pipeliteServices,
      StageExecutorResultCallback assertAfterSubmit,
      StageExecutorResultCallback assertAfterPoll) {

    String pipelineName = PipeliteIdCreator.pipelineName();
    String processId = PipeliteIdCreator.processId();
    String stageName = PipeliteIdCreator.stageName();
    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    executor.prepareExecution(pipeliteServices, pipelineName, processId, stage);

    StageExecutorResult result = executor.execute();

    while (result == null) {
      Time.wait(Duration.ofSeconds(1));
    }

    assertThat(result.isSubmitted()).isTrue();
    assertAfterSubmit.accept(result);

    while (!result.isSuccess() && !result.isError()) {
      result = executor.execute();
      Time.wait(Duration.ofSeconds(1));
    }

    // Ignore timeout errors.
    if (result.isTimeoutError()) {
      return;
    }

    assertAfterPoll.accept(result);
  }
}
