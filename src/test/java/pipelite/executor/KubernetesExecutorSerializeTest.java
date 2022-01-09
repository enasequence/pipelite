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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.json.Json;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;

public class KubernetesExecutorSerializeTest {

  @Test
  public void test() {
    String image = "debian";
    List<String> imageArgs = Arrays.asList("bash", "-c", "exit 1");
    KubernetesExecutor executor = StageExecutor.createKubernetesExecutor(image, imageArgs);
    Stage stage = Stage.builder().stageName("STAGE_NAME").executor(executor).build();

    executor.setContext("test");
    executor.setNamespace("test");
    executor.setJobName("test");
    String json = Json.serialize(executor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"image\" : \"debian\",\n"
                + "  \"imageArgs\" : [ \"bash\", \"-c\", \"exit 1\" ],\n"
                + "  \"context\" : \"test\",\n"
                + "  \"namespace\" : \"test\",\n"
                + "  \"jobName\" : \"test\"\n"
                + "}");
    KubernetesExecutor deserializedExecutor = Json.deserialize(json, KubernetesExecutor.class);
    assertThat(deserializedExecutor.getImage()).isEqualTo(image);
    assertThat(deserializedExecutor.getImageArgs()).containsExactly("bash", "-c", "exit 1");
    assertThat(deserializedExecutor.getContext()).isEqualTo("test");
    assertThat(deserializedExecutor.getNamespace()).isEqualTo("test");
    assertThat(deserializedExecutor.getJobName()).isEqualTo("test");
  }
}
