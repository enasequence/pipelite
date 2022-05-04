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
package pipelite.executor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import pipelite.PipeliteIdCreator;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;

public class LocalCmdExecutorTest {

  @Test
  public void test() {
    String stageName = PipeliteIdCreator.stageName();

    CmdExecutor<CmdExecutorParameters> executor = StageExecutor.createCmdExecutor("echo test");
    executor.setExecutorParams(CmdExecutorParameters.builder().build());
    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    stage.execute(
        (result) -> {
          assertThat(result.isSuccess()).isTrue();
          assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND))
              .isEqualTo("echo test");
          assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
          assertThat(result.getStageLog()).contains("test\n");
        });
  }
}
