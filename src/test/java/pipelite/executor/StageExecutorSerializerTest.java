package pipelite.executor;

import org.junit.jupiter.api.Test;
import pipelite.entity.StageEntity;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class StageExecutorSerializerTest {

  @Test
  public void deserializeExecutor() {
    for (StageExecutionResult result :
        Arrays.asList(
            StageExecutionResult.active(),
            StageExecutionResult.success(),
            StageExecutionResult.error())) {
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
            .executor(new EmptySyncStageExecutor(StageExecutionResultType.SUCCESS))
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
    for (StageExecutionResult result :
        Arrays.asList(
            StageExecutionResult.active(),
            StageExecutionResult.success(),
            StageExecutionResult.error())) {
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
