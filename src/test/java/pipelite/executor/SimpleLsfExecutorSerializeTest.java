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

import org.junit.jupiter.api.Test;
import pipelite.json.Json;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleLsfExecutorSerializeTest {

  @Test
  public void test() {
    String cmd = "echo test";
    SimpleLsfExecutor executor = StageExecutor.createSimpleLsfExecutor(cmd);
    Stage stage = Stage.builder().stageName("STAGE_NAME").executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName("PIPELINE_NAME")
            .processId("PROCESS_ID")
            .stage(stage)
            .build();

    executor.setJobId("test");
    executor.setOutFile(AbstractLsfExecutor.getOutFile(request, null));
    String json = Json.serialize(executor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"state\" : \"SUBMIT\",\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"outFile\" : \"pipelite/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.out\"\n"
                + "}");
    SimpleLsfExecutor deserializedLsfExecutor = Json.deserialize(json, SimpleLsfExecutor.class);
    assertThat(deserializedLsfExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getOutFile())
        .isEqualTo("pipelite/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.out");
  }
}
