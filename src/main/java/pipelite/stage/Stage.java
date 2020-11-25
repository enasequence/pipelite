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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.flogger.Flogger;
import pipelite.entity.StageEntity;
import pipelite.executor.StageExecutor;
import pipelite.executor.StageExecutorParameters;

@Flogger
@Data
public class Stage {
  private final String stageName;
  @EqualsAndHashCode.Exclude private StageExecutor executor;
  @EqualsAndHashCode.Exclude private StageExecutorParameters executorParams;
  @EqualsAndHashCode.Exclude private final List<Stage> dependsOn;
  @EqualsAndHashCode.Exclude private StageEntity stageEntity;
  @EqualsAndHashCode.Exclude private AtomicInteger immediateExecutionCount = new AtomicInteger();

  @Builder
  public Stage(
      String stageName,
      StageExecutor executor,
      StageExecutorParameters executorParams,
      List<Stage> dependsOn) {
    this.stageName = stageName;
    this.executor = executor;
    this.dependsOn = dependsOn;
    if (executorParams != null) {
      this.executorParams = executorParams;
    } else {
      this.executorParams = StageExecutorParameters.builder().build();
    }

    if (stageName == null || stageName.isEmpty()) {
      throw new IllegalArgumentException("Missing stage name");
    }
    if (executor == null) {
      throw new IllegalArgumentException("Missing executor");
    }
  }

  public StageExecutionResult execute(String pipelineName, String processId) {
    return executor.execute(pipelineName, processId, this);
  }

  public int getImmediateExecutionCount() {
    return immediateExecutionCount.get();
  }

  public void incrementImmediateExecutionCount() {
    immediateExecutionCount.incrementAndGet();
  }
}
