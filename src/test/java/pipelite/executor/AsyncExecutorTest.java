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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pipelite.PipeliteIdCreator;
import pipelite.stage.Stage;
import pipelite.stage.executor.ErrorType;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.time.Time;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

public class AsyncExecutorTest {

  private static final String STAGE_NAME = PipeliteIdCreator.stageName();

  private AsyncExecutor executor() {
    SimpleLsfExecutor simpleLsfExecutor =
        Mockito.spy(StageExecutor.createSimpleLsfExecutor("test"));
    simpleLsfExecutor.setExecutorParams(new SimpleLsfExecutorParameters());
    return simpleLsfExecutor;
  }

  @Test
  public void executeSubmitMissingJobId() {
    AsyncExecutor executor = executor();
    Stage stage = new Stage(STAGE_NAME, executor);

    executor.prepareExecution(null, "PIPELINE", "PROCESS", stage);
    doReturn(null).when(executor).submitJob();

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    executor.execute((r) -> result.set(r));

    while (result.get() == null) {
      Time.wait(Duration.ofSeconds(1));
    }
    assertThat(result.get().isError()).isTrue();
    assertThat(result.get().isErrorType(ErrorType.INTERNAL_ERROR)).isTrue();
    assertThat(result.get().getStageLog())
        .contains(
            "pipelite.exception.PipeliteSubmitException: Failed to submit async job PIPELINE PROCESS "
                + STAGE_NAME
                + ": missing job id");
  }

  @Test
  public void executeSubmitException() {
    AsyncExecutor executor = executor();
    doThrow(new RuntimeException("test exception")).when(executor).submitJob();

    Stage stage = new Stage(STAGE_NAME, executor);
    executor.prepareExecution(null, "PIPELINE", "PROCESS", stage);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    executor.execute((r) -> result.set(r));

    while (result.get() == null) {
      Time.wait(Duration.ofSeconds(1));
    }
    assertThat(result.get().isError()).isTrue();
    assertThat(result.get().isErrorType(ErrorType.INTERNAL_ERROR)).isTrue();
    assertThat(result.get().getStageLog()).contains("java.lang.RuntimeException: test exception");
  }
}
