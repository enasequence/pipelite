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
import static org.mockito.Mockito.mock;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import pipelite.entity.StageEntity;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.executor.SimpleLsfExecutor;
import pipelite.executor.SyncExecutor;
import pipelite.service.InternalErrorService;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

public class StageExecutorSerializerTest {

  private final InternalErrorService internalErrorService = mock(InternalErrorService.class);

  private static class TestExecutor extends SyncExecutor<ExecutorParameters>
      implements JsonSerializableExecutor {

    private StageState stageState;

    // Json deserialization requires no argument constructor.
    public TestExecutor() {}

    public TestExecutor(StageState stageState) {
      this.stageState = stageState;
    }

    @Override
    public void execute(StageExecutorRequest request, StageExecutorResultCallback resultCallback) {}

    @Override
    public void terminate() {}

    public StageState getStageState() {
      return stageState;
    }

    public void setStageState(StageState stageState) {
      this.stageState = stageState;
    }
  }

  @Test
  public void deserializeExecutorParams() {
    String json = "{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}";
    ExecutorParameters deserializedExecutorParams =
        StageExecutorSerializer.deserializeExecutorParameters(json, ExecutorParameters.class);
    assertThat(deserializedExecutorParams).isNotNull();
    assertThat(deserializedExecutorParams.getImmediateRetries()).isEqualTo(3);
    assertThat(deserializedExecutorParams.getMaximumRetries()).isEqualTo(3);
  }

  @Test
  public void deserializeSimpleLsfExecutorParamsBackwardsCompatibility() {
    String json =
        "{\n"
            + "  \"timeout\" : 432000000,\n"
            + "  \"maximumRetries\" : 6,\n"
            + "  \"immediateRetries\" : 2,\n"
            + "  \"host\" : \"yoda-login-2.ebi.ac.uk\",\n"
            + "  \"user\" : \"era\",\n"
            // workDir has been replaced by logDir
            + "  \"workDir\" : \"/hps/nobackup2/era/flow/log\",\n"
            // logBytes has been replaced by logLines
            + "  \"logBytes\" : 1000,\n"
            + "  \"permanentErrors\" : [ 52, 53, 54, 60, 74, 75 ],\n"
            + "  \"queue\" : \"standard\",\n"
            + "  \"cpu\" : 1,\n"
            + "  \"memory\" : 4352"
            + "}";
    SimpleLsfExecutorParameters deserializedExecutorParams =
        StageExecutorSerializer.deserializeExecutorParameters(
            json, SimpleLsfExecutorParameters.class);
    assertThat(deserializedExecutorParams).isNotNull();
    assertThat(deserializedExecutorParams.getTimeout()).isEqualTo(Duration.ofDays(5));
    assertThat(deserializedExecutorParams.getMaximumRetries()).isEqualTo(6);
    assertThat(deserializedExecutorParams.getImmediateRetries()).isEqualTo(2);
    assertThat(deserializedExecutorParams.getHost()).isEqualTo("yoda-login-2.ebi.ac.uk");
    assertThat(deserializedExecutorParams.getUser()).isEqualTo("era");
    assertThat(deserializedExecutorParams.getLogDir()).isEqualTo("/hps/nobackup2/era/flow/log");
    assertThat(deserializedExecutorParams.getPermanentErrors())
        .containsExactly(52, 53, 54, 60, 74, 75);
    assertThat(deserializedExecutorParams.getQueue()).isEqualTo("standard");
    assertThat(deserializedExecutorParams.getCpu()).isEqualTo(1);
    assertThat(deserializedExecutorParams.getMemory()).isEqualTo(4352);
  }

  @Test
  public void deserializeTestExecutor() {
    StageExecutorResult result = StageExecutorResult.success();
    StageEntity stageEntity = new StageEntity();

    TestExecutor testExecutor = new TestExecutor(StageState.from(result));

    Stage stage = Stage.builder().stageName("STAGE1").executor(testExecutor).build();
    stage.setStageEntity(stageEntity);

    ExecutorParameters params = new ExecutorParameters();
    params.setTimeout(Duration.ofMinutes(1));
    testExecutor.setExecutorParams(params);

    stageEntity.startExecution();
    StageService.prepareSaveStage(stage);

    assertThat(StageExecutorSerializer.deserializeExecutor(stage, internalErrorService)).isTrue();
    assertThat(stage.getExecutor()).isNotNull();
    assertThat(stage.getExecutor()).isInstanceOf(TestExecutor.class);
    assertThat(((TestExecutor) stage.getExecutor()).getStageState())
        .isEqualTo(StageState.from(result));
    assertThat(stage.getExecutor().getExecutorParams()).isNotNull();
    assertThat(stage.getExecutor().getExecutorParams().getImmediateRetries()).isEqualTo(3);
    assertThat(stage.getExecutor().getExecutorParams().getMaximumRetries()).isEqualTo(3);
    assertThat(stageEntity.getExecutorName())
        .isEqualTo("pipelite.stage.executor.StageExecutorSerializerTest$TestExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"stageState\" : \"SUCCESS\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 60000,\n"
                + "  \"maximumRetries\" : 3,\n"
                + "  \"immediateRetries\" : 3,\n"
                + "  \"logLines\" : 1000\n"
                + "}");
  }

  @Test
  public void deserializeSimpleLsfExecutor() {
    SimpleLsfExecutor lsfExecutor = StageExecutor.createSimpleLsfExecutor("test");
    lsfExecutor.setJobId("test");

    SimpleLsfExecutorParameters params = new SimpleLsfExecutorParameters();
    params.setHost("host");
    params.setQueue("queue");
    lsfExecutor.setExecutorParams(params);

    StageEntity stageEntity = new StageEntity();
    Stage stage = Stage.builder().stageName("STAGE1").executor(lsfExecutor).build();
    stage.setStageEntity(stageEntity);

    stageEntity.startExecution();
    StageService.prepareSaveStage(stage);

    assertThat(StageExecutorSerializer.deserializeExecutor(stage, internalErrorService)).isTrue();
    assertThat(stage.getExecutor()).isNotNull();
    assertThat(stage.getExecutor()).isInstanceOf(SimpleLsfExecutor.class);
    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SimpleLsfExecutor");
    assertThat(stageEntity.getExecutorData())
        .isEqualTo("{\n" + "  \"jobId\" : \"test\",\n" + "  \"cmd\" : \"test\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 604800000,\n"
                + "  \"maximumRetries\" : 3,\n"
                + "  \"immediateRetries\" : 3,\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"host\" : \"host\",\n"
                + "  \"logTimeout\" : 10000,\n"
                + "  \"queue\" : \"queue\"\n"
                + "}");
  }
}
