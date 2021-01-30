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

import com.google.common.flogger.FluentLogger;
import lombok.extern.flogger.Flogger;
import pipelite.entity.StageEntity;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.log.LogKey;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.parameters.ExecutorParameters;

@Flogger
public class StageExecutorSerializer {
  private StageExecutorSerializer() {}

  /**
   * Deserialize stage executor and stage parameters to allow an asynchronous executor to continue
   * executing an active stage.
   *
   * @oaran stage the stage being executed
   * @return true if the executor was deserialized
   */
  public static <T extends ExecutorParameters> Boolean deserializeExecution(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    if (StageState.isActive(stageEntity.getStageState())
        && stageEntity.getExecutorName() != null
        && stageEntity.getExecutorData() != null
        && stageEntity.getExecutorParams() != null) {
      StageExecutor deserializedExecutor = deserializeExecutor(stage);
      if (deserializedExecutor != null) {
        ExecutorParameters deserializedExecutorParams =
            deserializeExecutorParameters(stage, deserializedExecutor.getExecutorParamsType());
        if (deserializedExecutorParams != null) {
          logContext(log.atInfo(), stage).log("Using deserialized executor");
          stage.setExecutor(deserializedExecutor);
          deserializedExecutor.setExecutorParams(deserializedExecutorParams);
          return true;
        }
      }
    }
    return false;
  }

  /** Deserialize stage executor. */
  public static StageExecutor deserializeExecutor(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    try {
      if (stage.getExecutor() instanceof JsonSerializableExecutor) {
        return JsonSerializableExecutor.deserialize(
            stageEntity.getExecutorName(), stageEntity.getExecutorData());
      }
    } catch (Exception ex) {
      logContext(log.atSevere(), stage)
          .withCause(ex)
          .log("Failed to deserialize executor: %s", stageEntity.getExecutorName());
    }
    return null;
  }

  /** Deserialize stage executor parameters. */
  public static <T extends ExecutorParameters> T deserializeExecutorParameters(
      Stage stage, Class<T> cls) {
    StageEntity stageEntity = stage.getStageEntity();
    try {
      return ExecutorParameters.deserialize(stageEntity.getExecutorParams(), cls);
    } catch (Exception ex) {
      logContext(log.atSevere(), stage)
          .withCause(ex)
          .log("Failed to deserialize executor parameters: %s", stageEntity.getExecutorName());
    }
    return null;
  }

  private static FluentLogger.Api logContext(FluentLogger.Api log, Stage stage) {
    return log.with(LogKey.STAGE_NAME, stage.getStageName())
        .with(LogKey.EXECUTOR_NAME, stage.getExecutor().getClass().getName());
  }
}
