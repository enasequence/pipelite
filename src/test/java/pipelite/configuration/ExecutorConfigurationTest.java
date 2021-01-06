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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.PipeliteTestConfiguration;
import pipelite.stage.parameters.LsfExecutorParameters;

// TODO: move to and test YAML

@SpringBootTest(
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.executor.cmd.host=TEST",
      "pipelite.executor.cmd.immediateRetries=2",
      "pipelite.executor.cmd.maximumRetries=3",
      "pipelite.executor.cmd.timeout=10s",
      "pipelite.executor.lsf.definition=TEST",
      "pipelite.executor.lsf.format=YAML",
      "pipelite.executor.lsf.workdir=",
      "pipelite.executor.simpleLsf.memory=1",
      "pipelite.executor.simpleLsf.cpu=1",
      "pipelite.executor.simpleLsf.queue=TEST",
      "pipelite.executor.simpleLsf.memoryTimeout=15s",
      "pipelite.executor.simpleLsf.immediateRetries=2",
      "pipelite.executor.simpleLsf.maximumRetries=3",
      "pipelite.executor.simpleLsf.workdir=",
      "pipelite.executor.simpleLsf.timeout=10s",
      "pipelite.executor.awsBatch.region=TEST",
      "pipelite.executor.awsBatch.queue=TEST",
      "pipelite.executor.awsBatch.definition=TEST",
      "pipelite.executor.awsBatch.immediateRetries=2",
      "pipelite.executor.awsBatch.maximumRetries=3",
      "pipelite.executor.awsBatch.timeout=10s"
    })
public class ExecutorConfigurationTest {

  @Autowired ExecutorConfiguration config;

  @Test
  public void cmdProperties() {
    assertThat(config.getCmd().getHost()).isEqualTo("TEST");
    assertThat(config.getCmd().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getCmd().getMaximumRetries()).isEqualTo(3);
    assertThat(config.getCmd().getTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  public void lsfProperties() {
    assertThat(config.getLsf().getDefinition()).isEqualTo("TEST");
    assertThat(config.getLsf().getFormat()).isEqualTo(LsfExecutorParameters.Format.YAML);
    assertThat(config.getSimpleLsf().getWorkDir()).isBlank();
    assertThat(config.getSimpleLsf().getMemory()).isEqualTo(1);
    assertThat(config.getSimpleLsf().getCpu()).isEqualTo(1);
    assertThat(config.getSimpleLsf().getQueue()).isEqualTo("TEST");
    assertThat(config.getSimpleLsf().getMemoryTimeout().toMillis() / 1000L).isEqualTo(15);
    assertThat(config.getSimpleLsf().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getSimpleLsf().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getSimpleLsf().getMaximumRetries()).isEqualTo(3);
    assertThat(config.getSimpleLsf().getWorkDir()).isBlank();
    assertThat(config.getSimpleLsf().getTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  public void awsBatchProperties() {
    assertThat(config.getAwsBatch().getRegion()).isEqualTo("TEST");
    assertThat(config.getAwsBatch().getQueue()).isEqualTo("TEST");assertThat(config.getAwsBatch().getDefinition()).isEqualTo("TEST");
    assertThat(config.getAwsBatch().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getAwsBatch().getMaximumRetries()).isEqualTo(3);
    assertThat(config.getAwsBatch().getTimeout()).isEqualTo(Duration.ofSeconds(10));
  }
}
