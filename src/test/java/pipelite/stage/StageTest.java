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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import pipelite.executor.CallExecutor;

public class StageTest {

  @Test
  public void constructorThrowsMissingExecutorException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Stage.builder().stageName("TEST").executor(null).build();
        });
  }

  @Test
  public void constructorThrowsMissingStageNameException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Stage.builder().stageName(null).executor(new CallExecutor(StageState.SUCCESS)).build();
        });
  }

  @Test
  public void constructor() {
    Stage stage =
        Stage.builder().stageName("TEST").executor(new CallExecutor(StageState.SUCCESS)).build();
    assertThat(stage.getStageName()).isEqualTo("TEST");
    assertThat(stage.getExecutor()).isInstanceOf(CallExecutor.class);
  }
}
