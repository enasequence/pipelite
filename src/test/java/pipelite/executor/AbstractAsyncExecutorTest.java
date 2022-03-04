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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import java.util.EnumSet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pipelite.UniqueStringGenerator;
import pipelite.exception.PipeliteException;
import pipelite.executor.state.AsyncExecutorState;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

public class AbstractAsyncExecutorTest {

  private static final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String PROCESS_ID = UniqueStringGenerator.randomProcessId();
  private static final String STAGE_NAME = UniqueStringGenerator.randomStageName();

  private AbstractAsyncExecutor executor() {
    SimpleLsfExecutor simpleLsfExecutor =
        Mockito.spy(StageExecutor.createSimpleLsfExecutor("test"));
    simpleLsfExecutor.setExecutorParams(new SimpleLsfExecutorParameters());
    return simpleLsfExecutor;
  }

  @Test
  public void executeSubmitMissingJobId() {
    AbstractAsyncExecutor executor = executor();
    executor.setState(AsyncExecutorState.SUBMIT);
    doReturn(StageExecutorResult.submitted()).when(executor).submit(any());

    Stage stage = new Stage(STAGE_NAME, executor);
    StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);

    assertThatThrownBy(() -> executor.execute(request))
        .isInstanceOf(PipeliteException.class)
        .hasMessage("Missing job id after asynchronous submit");
  }

  @Test
  public void executeSubmitUnexpectedState() {
    AbstractAsyncExecutor executor = executor();
    for (StageExecutorState stageExecutorState :
        EnumSet.of(StageExecutorState.ACTIVE, StageExecutorState.SUCCESS)) {
      executor.setState(AsyncExecutorState.SUBMIT);
      doReturn(StageExecutorResult.from(stageExecutorState)).when(executor).submit(any());

      Stage stage = new Stage(STAGE_NAME, executor);
      StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);

      assertThatThrownBy(() -> executor.execute(request))
          .isInstanceOf(PipeliteException.class)
          .hasMessage("Unexpected state after asynchronous submit: " + stageExecutorState.name());
    }
  }

  @Test
  public void executeSubmitException() {
    AbstractAsyncExecutor executor = executor();
    executor.setState(AsyncExecutorState.SUBMIT);
    doThrow(new RuntimeException("test")).when(executor).submit(any());

    Stage stage = new Stage(STAGE_NAME, executor);
    StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);

    assertThatThrownBy(() -> executor.execute(request))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("test");
  }

  @Test
  public void executeSubmitError() {
    AbstractAsyncExecutor executor = executor();
    executor.setState(AsyncExecutorState.SUBMIT);
    doReturn(StageExecutorResult.error()).when(executor).submit(any());

    Stage stage = new Stage(STAGE_NAME, executor);
    StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);

    assertThat(executor.execute(request).isError()).isTrue();
  }

  @Test
  public void executePollStateMissingJobid() {
    AbstractAsyncExecutor executor = new SimpleLsfExecutor();
    executor.setState(AsyncExecutorState.POLL);
    Stage stage = new Stage(STAGE_NAME, executor);
    StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);
    assertThatThrownBy(() -> executor.execute(request))
        .isInstanceOf(PipeliteException.class)
        .hasMessage("Missing job id during asynchronous poll");
  }

  @Test
  public void executeDoneState() {
    AbstractAsyncExecutor executor = new SimpleLsfExecutor();
    executor.setState(AsyncExecutorState.DONE);
    Stage stage = new Stage(STAGE_NAME, executor);
    StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);
    assertThatThrownBy(() -> executor.execute(request))
        .isInstanceOf(PipeliteException.class)
        .hasMessage("Unexpected state during asynchronous execution: DONE");
  }

  @Test
  public void executeMissingState() {
    AbstractAsyncExecutor executor = new SimpleLsfExecutor();
    executor.setState(null);
    Stage stage = new Stage(STAGE_NAME, executor);
    StageExecutorRequest request = new StageExecutorRequest(PIPELINE_NAME, PROCESS_ID, stage);
    assertThatThrownBy(() -> executor.execute(request))
        .isInstanceOf(PipeliteException.class)
        .hasMessage("Missing state during asynchronous execution");
  }
}
