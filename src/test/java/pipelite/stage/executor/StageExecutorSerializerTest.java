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

import org.junit.jupiter.api.Test;
import pipelite.entity.StageEntity;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.executor.SimpleLsfExecutor;
import pipelite.executor.SyncExecutor;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

public class StageExecutorSerializerTest {

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
    StageEntity stageEntity = new StageEntity();
    stageEntity.setExecutorParams(
        "{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
    TestExecutor executor = new TestExecutor(StageState.SUCCESS);
    Stage stage = Stage.builder().stageName("STAGE1").executor(executor).build();
    stage.setStageEntity(stageEntity);
    ExecutorParameters deserializedExecutorParams =
        StageExecutorSerializer.deserializeExecutorParameters(
            stage, executor.getExecutorParamsType());
    assertThat(deserializedExecutorParams).isNotNull();
    assertThat(deserializedExecutorParams.getImmediateRetries()).isEqualTo(3);
    assertThat(deserializedExecutorParams.getMaximumRetries()).isEqualTo(3);
  }

  @Test
  public void deserializeExecutorJson() {
    StageExecutorResult result = StageExecutorResult.success();
    StageEntity stageEntity = new StageEntity();
    stageEntity.setExecutorName(TestExecutor.class.getName());
    stageEntity.setExecutorData(
        "{\n" + "  \"stageState\" : \"" + StageState.from(result).name() + "\"\n}");
    Stage stage =
        Stage.builder()
            .stageName("STAGE1")
            .executor(new TestExecutor(StageState.from(result)))
            .build();
    stage.setStageEntity(stageEntity);
    StageExecutor deserializedExecutor =
        StageExecutorSerializer.deserializeExecutor(
            stage, StageExecutorSerializer.Deserialize.JSON_EXECUTOR);
    assertThat(deserializedExecutor).isNotNull();
    assertThat(stage.getExecutor()).isInstanceOf(TestExecutor.class);
    assertThat(((TestExecutor) stage.getExecutor()).getStageState())
        .isEqualTo(StageState.from(result));
  }

  @Test
  public void deserializeExecutionJson() {
    StageExecutorResult result = StageExecutorResult.success();
    StageEntity stageEntity = new StageEntity();
    Stage stage =
        Stage.builder()
            .stageName("STAGE1")
            .executor(new TestExecutor(StageState.from(result)))
            .build();
    stage.setStageEntity(stageEntity);
    stageEntity.startExecution();
    stageEntity.setExecutorName(TestExecutor.class.getName());
    stageEntity.setExecutorData(
        "{\n" + "  \"stageState\" : \"" + StageState.from(result).name() + "\"\n}");
    stageEntity.setExecutorParams(
        "{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
    assertThat(
            StageExecutorSerializer.deserializeExecution(
                stage, StageExecutorSerializer.Deserialize.JSON_EXECUTOR))
        .isTrue();
    assertThat(stage.getExecutor()).isNotNull();
    assertThat(stage.getExecutor()).isInstanceOf(TestExecutor.class);
    assertThat(((TestExecutor) stage.getExecutor()).getStageState())
        .isEqualTo(StageState.from(result));
    assertThat(stage.getExecutor().getExecutorParams()).isNotNull();
    assertThat(stage.getExecutor().getExecutorParams().getImmediateRetries()).isEqualTo(3);
    assertThat(stage.getExecutor().getExecutorParams().getMaximumRetries()).isEqualTo(3);
  }

  @Test
  public void deserializeExecutionAsync() {
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

    assertThat(
            StageExecutorSerializer.deserializeExecution(
                stage, StageExecutorSerializer.Deserialize.ASYNC_EXECUTOR))
        .isTrue();
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
