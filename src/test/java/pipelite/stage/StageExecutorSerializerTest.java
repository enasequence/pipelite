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
package pipelite.stage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import pipelite.entity.StageEntity;
import pipelite.stage.*;
import pipelite.stage.executor.*;

public class StageExecutorSerializerTest {

  @Test
  public void deserializeExecutor() {
    for (StageExecutorResult result :
        Arrays.asList(
            StageExecutorResult.active(),
            StageExecutorResult.success(),
            StageExecutorResult.error())) {
      StageEntity stageEntity = new StageEntity();
      stageEntity.setExecutorName(EmptySyncStageExecutor.class.getName());
      stageEntity.setExecutorData(
          "{\n" + "  \"resultType\" : \"" + result.getResultType().name() + "\"\n}");
      Stage stage =
          Stage.builder()
              .stageName("STAGE1")
              .executor(new EmptySyncStageExecutor(result.getResultType()))
              .build();
      stage.setStageEntity(stageEntity);
      StageExecutor deserializedExecutor = StageExecutorSerializer.deserializeExecutor(stage);
      assertThat(deserializedExecutor).isNotNull();
      assertThat(deserializedExecutor)
          .isEqualTo(new EmptySyncStageExecutor(result.getResultType()));
    }
  }

  @Test
  public void deserializeExecutorParams() {
    StageEntity stageEntity = new StageEntity();
    stageEntity.setExecutorParams(
        "{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
    Stage stage =
        Stage.builder()
            .stageName("STAGE1")
            .executor(new EmptySyncStageExecutor(StageExecutorResultType.SUCCESS))
            .build();
    stage.setStageEntity(stageEntity);
    StageExecutorParameters deserializedExecutorParams =
        StageExecutorSerializer.deserializeExecutorParameters(stage);
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
              .executor(new EmptySyncStageExecutor(result.getResultType()))
              .build();
      stage.setStageEntity(stageEntity);
      stageEntity.startExecution(stage);
      stageEntity.setExecutorName(EmptySyncStageExecutor.class.getName());
      stageEntity.setExecutorData(
          "{\n" + "  \"resultType\" : \"" + result.getResultType().name() + "\"\n}");
      stageEntity.setExecutorParams(
          "{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
      assertThat(StageExecutorSerializer.deserializeExecution(stage)).isTrue();
      assertThat(stage.getExecutor()).isNotNull();
      assertThat(stage.getExecutor()).isEqualTo(new EmptySyncStageExecutor(result.getResultType()));
      assertThat(stage.getExecutorParams()).isNotNull();
      assertThat(stage.getExecutorParams().getImmediateRetries()).isEqualTo(3);
      assertThat(stage.getExecutorParams().getMaximumRetries()).isEqualTo(3);
    }
  }
}
