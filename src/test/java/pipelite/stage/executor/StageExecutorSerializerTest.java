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

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import pipelite.entity.StageEntity;
import pipelite.executor.AbstractExecutor;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.parameters.ExecutorParameters;

public class StageExecutorSerializerTest {

  private static class TestExecutor extends AbstractExecutor implements JsonSerializableExecutor {

    private StageState stageState;

    // Json deserialization requires no argument constructor.
    public TestExecutor() {}

    public TestExecutor(StageState stageState) {
      this.stageState = stageState;
    }

    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      return null;
    }

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
  public void deserializeExecutor() {
    for (StageExecutorResult result :
        Arrays.asList(
            StageExecutorResult.active(),
            StageExecutorResult.success(),
            StageExecutorResult.error())) {
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
      StageExecutor deserializedExecutor = StageExecutorSerializer.deserializeExecutor(stage);
      assertThat(deserializedExecutor).isNotNull();
      assertThat(stage.getExecutor()).isInstanceOf(TestExecutor.class);
      assertThat(((TestExecutor) stage.getExecutor()).getStageState())
          .isEqualTo(StageState.from(result));
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
  public void deserializeExecution() {
    for (StageExecutorResult result :
        Arrays.asList(
            StageExecutorResult.active(),
            StageExecutorResult.success(),
            StageExecutorResult.error())) {
      StageEntity stageEntity = new StageEntity();
      Stage stage =
          Stage.builder()
              .stageName("STAGE1")
              .executor(new TestExecutor(StageState.from(result)))
              .build();
      stage.setStageEntity(stageEntity);
      stageEntity.startExecution(stage);
      stageEntity.setExecutorName(TestExecutor.class.getName());
      stageEntity.setExecutorData(
          "{\n" + "  \"stageState\" : \"" + StageState.from(result).name() + "\"\n}");
      stageEntity.setExecutorParams(
          "{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
      assertThat(StageExecutorSerializer.deserializeExecution(stage)).isTrue();
      assertThat(stage.getExecutor()).isNotNull();
      assertThat(stage.getExecutor()).isInstanceOf(TestExecutor.class);
      assertThat(((TestExecutor) stage.getExecutor()).getStageState())
          .isEqualTo(StageState.from(result));
      assertThat(stage.getExecutor().getExecutorParams()).isNotNull();
      assertThat(stage.getExecutor().getExecutorParams().getImmediateRetries()).isEqualTo(3);
      assertThat(stage.getExecutor().getExecutorParams().getMaximumRetries()).isEqualTo(3);
    }
  }
}
