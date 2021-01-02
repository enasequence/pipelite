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
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;

// TODO: move to and test YAML

@SpringBootTest(
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.executor.cmd.host=TEST",
      "pipelite.executor.cmd.immediateRetries=2",
      "pipelite.executor.cmd.maximumRetries=3",
      "pipelite.executor.cmd.timeout=10s",
      "pipelite.executor.lsf.memory=1",
      "pipelite.executor.lsf.cores=1",
      "pipelite.executor.lsf.queue=TEST",
      "pipelite.executor.lsf.memoryTimeout=15s",
      "pipelite.executor.lsf.immediateRetries=2",
      "pipelite.executor.lsf.maximumRetries=3",
      "pipelite.executor.lsf.workdir=",
      "pipelite.executor.lsf.timeout=10s",
      "pipelite.executor.awsBatch.region=TEST",
      "pipelite.executor.awsBatch.queue=TEST",
      "pipelite.executor.awsBatch.jobDefinition=TEST",
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
    assertThat(config.getLsf().getMemory()).isEqualTo(1);
    assertThat(config.getLsf().getCores()).isEqualTo(1);
    assertThat(config.getLsf().getQueue()).isEqualTo("TEST");
    assertThat(config.getLsf().getMemoryTimeout().toMillis() / 1000L).isEqualTo(15);
    assertThat(config.getLsf().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getLsf().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getLsf().getMaximumRetries()).isEqualTo(3);
    assertThat(config.getLsf().getWorkDir()).isBlank();
    assertThat(config.getLsf().getTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  public void awsBatchProperties() {
    assertThat(config.getAwsBatch().getRegion()).isEqualTo("TEST");
    assertThat(config.getAwsBatch().getQueue()).isEqualTo("TEST");
    assertThat(config.getAwsBatch().getJobDefinition()).isEqualTo("TEST");
    // TODO: test map using YAML
    // assertThat(config.getAwsBatch().getJobParameters()).isEqualTo("TEST");
    assertThat(config.getAwsBatch().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getAwsBatch().getMaximumRetries()).isEqualTo(3);
    assertThat(config.getAwsBatch().getTimeout()).isEqualTo(Duration.ofSeconds(10));
  }
}
