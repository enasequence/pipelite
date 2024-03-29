/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.stage;

import java.util.concurrent.atomic.AtomicInteger;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.flogger.Flogger;
import pipelite.entity.StageEntity;
import pipelite.entity.field.StageState;
import pipelite.runner.stage.StageRunner;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;

@Flogger
@Data
public class Stage {
  private final String stageName;
  @EqualsAndHashCode.Exclude private StageExecutor executor;
  @EqualsAndHashCode.Exclude private StageEntity stageEntity;
  @EqualsAndHashCode.Exclude private AtomicInteger immediateExecutionCount = new AtomicInteger();

  @Builder
  public Stage(String stageName, StageExecutor executor) {
    this.stageName = stageName;
    this.executor = executor;

    if (stageName == null || stageName.isEmpty()) {
      throw new IllegalArgumentException("Missing stage name");
    }
    if (executor == null) {
      throw new IllegalArgumentException("Missing executor");
    }
  }

  public StageExecutorResult execute() {
    return executor.execute();
  }

  public int getImmediateExecutionCount() {
    return immediateExecutionCount.get();
  }

  public void incrementImmediateExecutionCount() {
    immediateExecutionCount.incrementAndGet();
  }

  public boolean isActive() {
    return stageEntity.getStageState() == StageState.ACTIVE;
  }

  public boolean isSuccess() {
    return stageEntity.getStageState() == StageState.SUCCESS;
  }

  public boolean isError() {
    return stageEntity.getStageState() == StageState.ERROR;
  }

  public boolean isPermanentError() {
    return isError()
        && stageEntity.getErrorType() != null
        && stageEntity.getErrorType().isPermanentError();
  }

  /**
   * Returns true if the stage has maximum retries left.
   *
   * @return true if the stage has maximum retries left.
   */
  public boolean hasMaximumRetriesLeft() {
    // Stage has been retried the maximum number of times.
    return stageEntity.getExecutionCount() <= StageRunner.getMaximumRetries(this);
  }

  /**
   * Returns true if the stage has immediate retries left.
   *
   * @return true if the stage has immediate retries left.
   */
  public boolean hasImmediateRetriesLeft() {
    return immediateExecutionCount.get() <= StageRunner.getImmediateRetries(this);
  }
}
