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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pipelite.PipeliteIdCreator;
import pipelite.service.PipeliteExecutorService;
import pipelite.stage.Stage;
import pipelite.stage.executor.*;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.time.Time;

public class AsyncExecutorTest {

  private static final String PIPELINE_NAME = PipeliteIdCreator.pipelineName();
  private static final String PROCESS_ID = PipeliteIdCreator.processId();
  private static final String STAGE_NAME = PipeliteIdCreator.stageName();

  private AsyncExecutor executor() {
    SimpleLsfExecutor simpleLsfExecutor =
        Mockito.spy(StageExecutor.createSimpleLsfExecutor("test"));
    simpleLsfExecutor.setExecutorParams(new SimpleLsfExecutorParameters());
    simpleLsfExecutor.setSubmitExecutorService(
        PipeliteExecutorService.createExecutorService("test-%d", 10, null));
    return simpleLsfExecutor;
  }

  @Test
  public void executeSubmitMissingJobId() {
    AsyncExecutor executor = executor();
    doReturn(new AsyncExecutor.SubmitResult(null, StageExecutorResult.submitted()))
        .when(executor)
        .submit(any());

    Stage stage = new Stage(STAGE_NAME, executor);
    StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    executor.execute(request, (r) -> result.set(r));

    while (result.get() == null) {
      Time.wait(Duration.ofSeconds(1));
    }
    assertThat(result.get().isError()).isTrue();
    assertThat(result.get().isErrorType(ErrorType.INTERNAL_ERROR)).isTrue();
    assertThat(result.get().getStageLog())
        .contains("PipeliteException: Missing job id after asynchronous submit");
  }

  @Test
  public void executeSubmitUnexpectedState() {
    for (StageExecutorState stageExecutorState :
        EnumSet.of(StageExecutorState.ACTIVE, StageExecutorState.SUCCESS)) {
      AsyncExecutor executor = executor();
      doReturn(
              new AsyncExecutor.SubmitResult("jobId", StageExecutorResult.from(stageExecutorState)))
          .when(executor)
          .submit(any());

      Stage stage = new Stage(STAGE_NAME, executor);
      StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);

      AtomicReference<StageExecutorResult> result = new AtomicReference<>();
      executor.execute(request, (r) -> result.set(r));

      while (result.get() == null) {
        Time.wait(Duration.ofSeconds(1));
      }
      assertThat(result.get().isError()).isTrue();
      assertThat(result.get().isErrorType(ErrorType.INTERNAL_ERROR)).isTrue();
      assertThat(result.get().getStageLog())
          .contains("PipeliteException: Unexpected state after asynchronous submit");
    }
  }

  @Test
  public void executeSubmitException() {
    AsyncExecutor executor = executor();
    doThrow(new RuntimeException("test exception")).when(executor).submit(any());

    Stage stage = new Stage(STAGE_NAME, executor);
    StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    executor.execute(request, (r) -> result.set(r));

    while (result.get() == null) {
      Time.wait(Duration.ofSeconds(1));
    }
    assertThat(result.get().isError()).isTrue();
    assertThat(result.get().isErrorType(ErrorType.INTERNAL_ERROR)).isTrue();
    assertThat(result.get().getStageLog()).contains("java.lang.RuntimeException: test exception");
  }

  @Test
  public void executeSubmitError() {
    AsyncExecutor executor = executor();
    doReturn(new AsyncExecutor.SubmitResult(null, StageExecutorResult.error()))
        .when(executor)
        .submit(any());

    Stage stage = new Stage(STAGE_NAME, executor);
    StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    executor.execute(request, (r) -> result.set(r));

    while (result.get() == null) {
      Time.wait(Duration.ofSeconds(1));
    }
    assertThat(result.get().isError()).isTrue();
  }
}
