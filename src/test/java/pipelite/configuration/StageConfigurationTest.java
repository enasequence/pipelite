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
package pipelite.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.TestConfiguration;

@SpringBootTest(
    classes = TestConfiguration.class,
    properties = {
      "pipelite.stage.memory=1",
      "pipelite.stage.cores=1",
      "pipelite.stage.queue=TEST",
      "pipelite.stage.memoryTimeout=15s",
      "pipelite.stage.retries=3",
      "pipelite.stage.tempdir=",
      "pipelite.stage.env=TEST1,TEST2"
    })
public class StageConfigurationTest {

  @Autowired StageConfiguration config;

  @Test
  public void test() {
    assertThat(config.getMemory()).isEqualTo(1);
    assertThat(config.getCores()).isEqualTo(1);
    assertThat(config.getQueue()).isEqualTo("TEST");
    assertThat(config.getMemoryTimeout().toMillis() / 1000L).isEqualTo(15);
    assertThat(config.getRetries()).isEqualTo(3);
    assertThat(config.getWorkDir()).isBlank();
    assertThat(config.getEnv().length).isEqualTo(2);
    assertThat(config.getEnv()[0]).isEqualTo("TEST1");
    assertThat(config.getEnv()[1]).isEqualTo("TEST2");
  }
}
