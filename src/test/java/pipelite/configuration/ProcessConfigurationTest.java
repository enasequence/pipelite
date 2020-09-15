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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.EmptyTestConfiguration;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {"pipelite.process.pipelineName=TEST", "pipelite.process.processFactoryName=TEST"})
@EnableConfigurationProperties(value = {ProcessConfiguration.class})
public class ProcessConfigurationTest {

  @Autowired ProcessConfiguration config;

  @Test
  public void test() {
    assertThat(config.getPipelineName()).isEqualTo("TEST");
    assertThat(config.getProcessFactoryName()).isEqualTo("TEST");
  }
}
