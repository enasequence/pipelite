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
package pipelite.executor;

import org.junit.jupiter.api.Test;
import pipelite.json.Json;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.path.FilePathResolverTestHelper;
import pipelite.stage.path.SlurmLogFilePathResolver;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleSlurmExecutorSerializeTest {

  @Test
  public void test() {
    String cmd = "echo test";
    SimpleSlurmExecutor executor = StageExecutor.createSimpleSlurmExecutor(cmd);
    Stage stage = Stage.builder().stageName("STAGE_NAME").executor(executor).build();

    executor.setJobId("test");
    executor.setOutFile(
        new SlurmLogFilePathResolver()
            .resolvedPath()
            .file(
                FilePathResolverTestHelper.request(
                    "PIPELINE_NAME", "PROCESS_ID", stage.getStageName(), "user", "logDir")));

    String json = Json.serialize(executor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"outFile\" : \"logDir/user/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.out\"\n"
                + "}");
    SimpleSlurmExecutor deserializedSlurmExecutor = Json.deserialize(json, SimpleSlurmExecutor.class);
    assertThat(deserializedSlurmExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedSlurmExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedSlurmExecutor.getOutFile())
        .isEqualTo("logDir/user/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.out");
  }
}
