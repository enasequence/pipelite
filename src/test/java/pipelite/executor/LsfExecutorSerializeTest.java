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
import pipelite.json.Json;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.path.FilePathResolverTestHelper;
import pipelite.stage.path.LsfLogFilePathResolver;

public class LsfExecutorSerializeTest {

  @Test
  public void test() {
    String cmd = "echo test";
    LsfExecutor executor = StageExecutor.createLsfExecutor(cmd);
    Stage stage = Stage.builder().stageName("STAGE_NAME").executor(executor).build();

    StageExecutorRequest request =
        FilePathResolverTestHelper.request(
            "PIPELINE_NAME", "PROCESS_ID", stage.getStageName(), "user", "logDir");

    executor.setJobId("test");
    executor.setOutFile(new LsfLogFilePathResolver().resolvedPath().file(request));
    executor.setDefinitionFile("tempFile");
    String json = Json.serialize(executor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"outFile\" : \"logDir/user/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.out\",\n"
                + "  \"definitionFile\" : \"tempFile\"\n"
                + "}");
    LsfExecutor deserializedLsfExecutor = Json.deserialize(json, LsfExecutor.class);
    assertThat(deserializedLsfExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getOutFile())
        .isEqualTo("logDir/user/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.out");
    assertThat(deserializedLsfExecutor.getDefinitionFile()).isEqualTo("tempFile");
  }
}
