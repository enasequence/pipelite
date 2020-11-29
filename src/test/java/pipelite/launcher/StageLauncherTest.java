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
package pipelite.launcher;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import pipelite.configuration.StageConfiguration;
import pipelite.executor.StageExecutorParameters;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;

public class StageLauncherTest {

  private void testMaximumRetries(Integer maximumRetries, int expectedMaximumRetries) {
    assertThat(
            StageLauncher.getMaximumRetries(
                Stage.builder()
                    .stageName("STAGE")
                    .executor((pipelineName, processId, stage) -> StageExecutionResult.success())
                    .executorParams(
                        StageExecutorParameters.builder().maximumRetries(maximumRetries).build())
                    .build()))
        .isEqualTo(expectedMaximumRetries);
  }

  private void testImmediateRetries(
      Integer immediateRetries, Integer maximumRetries, int expectedImmediateRetries) {
    assertThat(
            StageLauncher.getImmediateRetries(
                Stage.builder()
                    .stageName("STAGE")
                    .executor((pipelineName, processId, stage) -> StageExecutionResult.success())
                    .executorParams(
                        StageExecutorParameters.builder()
                            .maximumRetries(maximumRetries)
                            .immediateRetries(immediateRetries)
                            .build())
                    .build()))
        .isEqualTo(expectedImmediateRetries);
  }

  @Test
  public void maximumRetries() {
    testMaximumRetries(1, 1);
    testMaximumRetries(5, 5);
    testMaximumRetries(null, StageConfiguration.DEFAULT_MAX_RETRIES);
  }

  @Test
  public void immediateRetries() {
    testImmediateRetries(3, 6, 3);
    testImmediateRetries(3, 2, 2);
    testImmediateRetries(3, 0, 0);
    testImmediateRetries(
        null,
        StageConfiguration.DEFAULT_IMMEDIATE_RETRIES + 1,
        StageConfiguration.DEFAULT_IMMEDIATE_RETRIES);
    testImmediateRetries(null, null, StageConfiguration.DEFAULT_IMMEDIATE_RETRIES);
  }
}
