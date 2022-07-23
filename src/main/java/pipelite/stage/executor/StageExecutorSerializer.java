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
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.flogger.Flogger;
import pipelite.entity.StageEntity;
import pipelite.error.InternalErrorHandler;
import pipelite.exception.PipeliteException;
import pipelite.executor.JsonSerializableExecutor;
import pipelite.log.LogKey;
import pipelite.service.InternalErrorService;
import pipelite.stage.Stage;
import pipelite.stage.parameters.ExecutorParameters;

@Flogger
public class StageExecutorSerializer {
  private StageExecutorSerializer() {}

  /**
   * Deserialize the stage executor.
   *
   * @oaran stage the stage
   * @return true if the stage executor was deserialized
   */
  public static <T extends ExecutorParameters> boolean deserializeExecutor(
      Stage stage, InternalErrorService internalErrorService) {
    if (!(stage.getExecutor() instanceof JsonSerializableExecutor)) {
      return false;
    }

    String pipelineName = stage.getStageEntity().getPipelineName();
    String processId = stage.getStageEntity().getProcessId();
    String stageName = stage.getStageName();

    logContext(log.atInfo(), pipelineName, processId, stageName).log("Deserialize stage executor");

    InternalErrorHandler internalErrorHandler =
        new InternalErrorHandler(
            internalErrorService,
            pipelineName,
            processId,
            stageName,
            StageExecutorSerializer.class);

    AtomicBoolean isDeserialize = new AtomicBoolean(false);
    // Unexpected exceptions are logged as internal errors but otherwise ignored when deserializing
    // the executor.
    internalErrorHandler.execute(
        () -> {
          StageEntity stageEntity = stage.getStageEntity();
          if (stageEntity.getExecutorName() == null) {
            throw new PipeliteException(
                "Failed to deserialize stage executor because of missing executor name");
          }
          if (stageEntity.getExecutorData() == null) {
            throw new PipeliteException(
                "Failed to deserialize stage executor because of missing executor data");
          }
          if (stageEntity.getExecutorParams() == null) {
            throw new PipeliteException(
                "Failed to deserialize stage executor because of missing executor parameters");
          }

          StageExecutor deserializedExecutor = deserializeExecutorData(stage);
          if (deserializedExecutor == null) {
            throw new PipeliteException(
                "Failed to deserialize stage executor because executor data could not be deserialized");
          }

          if (!deserializedExecutor.isSubmitted()) {
            logContext(log.atInfo(), pipelineName, processId, stageName)
                .log("Did not deserialize stage executor because stage has not been submitted");
            return;
          }

          ExecutorParameters deserializedExecutorParams =
              deserializeExecutorParameters(
                  stage.getStageEntity().getExecutorParams(),
                  deserializedExecutor.getExecutorParamsType());
          if (deserializedExecutorParams == null) {
            throw new PipeliteException(
                "Failed to deserialize stage executor because executor parameters could not be deserialized");
          }

          logContext(log.atInfo(), pipelineName, processId, stageName)
              .log("Deserialized stage executor");

          isDeserialize.set(true);
          deserializedExecutor.setExecutorParams(deserializedExecutorParams);
          stage.setExecutor(deserializedExecutor);
        });

    return isDeserialize.get();
  }

  /** Deserialize stage executor data. */
  public static StageExecutor deserializeExecutorData(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    try {
      return JsonSerializableExecutor.deserialize(
          stageEntity.getExecutorName(), stageEntity.getExecutorData());
    } catch (Exception ex) {
      throw new PipeliteException(
          "Failed to deserialize stage executor because executor data could not be deserialized: "
              + ex.getMessage());
    }
  }

  /** Deserialize stage executor parameters. */
  public static <T extends ExecutorParameters> T deserializeExecutorParameters(
      String json, Class<T> cls) {
    try {
      return ExecutorParameters.deserialize(json, cls);
    } catch (Exception ex) {
      throw new PipeliteException(
          "Failed to deserialize stage executor because executor parameters could not be deserialized: "
              + ex.getMessage());
    }
  }

  private static FluentLogger.Api logContext(
      FluentLogger.Api log, String pipelineName, String processId, String stageName) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stageName);
  }
}
