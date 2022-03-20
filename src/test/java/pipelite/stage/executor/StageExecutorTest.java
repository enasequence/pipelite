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
package pipelite.stage.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.stage.parameters.cmd.LogFileSavePolicy.ERROR;
import static pipelite.stage.parameters.cmd.LogFileSavePolicy.NEVER;

import org.junit.jupiter.api.Test;
import pipelite.executor.SyncExecutor;
import pipelite.service.PipeliteServices;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.stage.parameters.cmd.LogFileSavePolicy;

public class StageExecutorTest {

  private static class TestExecutor extends SyncExecutor<ExecutorParameters> {

    private final ExecutorParameters params;

    public TestExecutor(LogFileSavePolicy savePolicy) {
      params = ExecutorParameters.builder().logSave(savePolicy).build();
    }

    @Override
    public Class getExecutorParamsType() {
      return null;
    }

    @Override
    public ExecutorParameters getExecutorParams() {
      return params;
    }

    @Override
    public void setExecutorParams(ExecutorParameters executorParams) {}

    @Override
    public void prepareExecution(
        PipeliteServices pipeliteServices,
        String pipelineName,
        String processId,
        String stageName) {}

    @Override
    public void execute(StageExecutorRequest request, StageExecutorResultCallback resultCallback) {}

    @Override
    public void terminate() {}
  }

  private boolean isSaveLogFile(LogFileSavePolicy policy, StageExecutorResult result) {
    TestExecutor testExecutor = new TestExecutor(policy);
    return testExecutor.isSaveLogFile(result);
  }

  private StageExecutorResult error() {
    return StageExecutorResult.error();
  }

  private StageExecutorResult success() {
    return StageExecutorResult.success();
  }

  @Test
  public void testIsSaveLogFile() {
    assertThat(isSaveLogFile(NEVER, error())).isFalse();
    assertThat(isSaveLogFile(NEVER, success())).isFalse();

    assertThat(isSaveLogFile(ERROR, error())).isTrue();
    assertThat(isSaveLogFile(ERROR, success())).isFalse();

    assertThat(isSaveLogFile(LogFileSavePolicy.ALWAYS, error())).isTrue();
    assertThat(isSaveLogFile(LogFileSavePolicy.ALWAYS, success())).isTrue();
  }
}
