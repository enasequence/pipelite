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
package pipelite.executor.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.json.Json;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;
import pipelite.stage.StageParameters;

public class LocalCmdExecutorTest {

  @Test
  public void test() {

    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    String stageName = UniqueStringGenerator.randomStageName();

    StageParameters stageParameters = StageParameters.builder().build();

    Stage stage =
        Stage.builder()
            .pipelineName(pipelineName)
            .processId(processId)
            .stageName(stageName)
            .executor(new LocalCmdExecutor("echo test"))
            .stageParameters(stageParameters)
            .build();

    StageExecutionResult result = stage.execute();
    assertThat(result.getResultType()).isEqualTo(StageExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(StageExecutionResult.COMMAND)).isEqualTo("echo test");
    assertThat(result.getAttribute(StageExecutionResult.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStdout()).isEqualTo("test\n");
  }

  @Test
  public void serialize() {
    String cmd = "echo test";
    String json = Json.serialize(new LocalCmdExecutor(cmd));
    assertThat(json).isEqualTo("{\n" + "  \"cmd\" : \"echo test\"\n" + "}");
    assertThat(Json.deserialize(json, LocalCmdExecutor.class).getCmd()).isEqualTo(cmd);
  }
}
