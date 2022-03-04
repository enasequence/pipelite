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
import pipelite.executor.AbstractAsyncExecutor;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.executor.state.AsyncExecutorState;
import pipelite.log.LogKey;
import pipelite.stage.Stage;
import pipelite.stage.parameters.ExecutorParameters;

@Flogger
public class StageExecutorSerializer {
  private StageExecutorSerializer() {}

  public enum Deserialize {
    /** Deserialize JsonSerializableExecutor. */
    JSON_EXECUTOR,
    /** Deserialize AbstractAsyncExecutor with state AsyncExecutorState.POLL. */
    ASYNC_EXECUTOR_POLL;
  }

  /**
   * Deserialize stage executor and stage executor parameters. An asynchronous executor must be in
   * the POLL state to be deserialized.
   *
   * @oaran stage the stage being executed
   * @return true if the executor was deserialized
   */
  public static <T extends ExecutorParameters> Boolean deserializeExecution(
      Stage stage, Deserialize deserialize) {
    if (!(stage.getExecutor() instanceof JsonSerializableExecutor)) {
      return false;
    }
    StageEntity stageEntity = stage.getStageEntity();
    if (stageEntity.getExecutorName() == null
        || stageEntity.getExecutorData() == null
        || stageEntity.getExecutorParams() == null) {
      return false;
    }
    StageExecutor deserializedExecutor = deserializeExecutor(stage, deserialize);
    if (deserializedExecutor == null) {
      logContext(log.atSevere(), stage).log("Failed to deserialize executor");
      return false;
    }

    if (deserialize == Deserialize.ASYNC_EXECUTOR_POLL) {
      AbstractAsyncExecutor deserializedAsyncExecutor =
          (AbstractAsyncExecutor) deserializedExecutor;
      // Backward compatible with version < 1.4.* without AsyncExecutorState.
      if (deserializedAsyncExecutor.getState() == null
          && deserializedAsyncExecutor.getJobId() != null) {
        deserializedAsyncExecutor.setState(AsyncExecutorState.POLL);
      }
      if (deserializedAsyncExecutor.getState() != AsyncExecutorState.POLL) {
        return false;
      }
    }

    ExecutorParameters deserializedExecutorParams =
        deserializeExecutorParameters(stage, deserializedExecutor.getExecutorParamsType());
    if (deserializedExecutorParams == null) {
      return false;
    }

    logContext(log.atInfo(), stage).log("Using deserialized executor");
    stage.setExecutor(deserializedExecutor);
    deserializedExecutor.setExecutorParams(deserializedExecutorParams);
    return true;
  }

  /** Deserialize stage executor. */
  public static StageExecutor deserializeExecutor(Stage stage, Deserialize deserialize) {
    StageEntity stageEntity = stage.getStageEntity();
    try {
      boolean action = false;
      switch (deserialize) {
        case JSON_EXECUTOR:
          action = stage.getExecutor() instanceof JsonSerializableExecutor;
          break;
        case ASYNC_EXECUTOR_POLL:
          action = stage.getExecutor() instanceof AbstractAsyncExecutor;
          break;
      }
      if (action) {
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
