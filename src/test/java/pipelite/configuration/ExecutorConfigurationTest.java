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
package pipelite.configuration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithConfigurations;

// TODO: move to and test YAML

@SpringBootTest(
    classes = PipeliteTestConfigWithConfigurations.class,
    properties = {
      // CMD
      "pipelite.executor.cmd.user=TEST",
      "pipelite.executor.cmd.host=TEST",
      "pipelite.executor.cmd.immediateRetries=2",
      "pipelite.executor.cmd.maximumRetries=3",
      "pipelite.executor.cmd.timeout=10s",
      // LSF
      "pipelite.executor.simpleLsf.user=TEST",
      "pipelite.executor.simpleLsf.host=TEST",
      "pipelite.executor.simpleLsf.memory=1",
      "pipelite.executor.simpleLsf.cpu=1",
      "pipelite.executor.simpleLsf.queue=TEST",
      "pipelite.executor.simpleLsf.memoryTimeout=15s",
      "pipelite.executor.simpleLsf.immediateRetries=2",
      "pipelite.executor.simpleLsf.maximumRetries=3",
      "pipelite.executor.simpleLsf.timeout=10s",
      "pipelite.executor.simpleLsf.logDir=TEST_DIR1",
      // SLURM
      "pipelite.executor.simpleSlurm.user=TEST",
      "pipelite.executor.simpleSlurm.host=TEST",
      "pipelite.executor.simpleSlurm.memory=1",
      "pipelite.executor.simpleSlurm.cpu=1",
      "pipelite.executor.simpleSlurm.queue=TEST",
      "pipelite.executor.simpleSlurm.immediateRetries=2",
      "pipelite.executor.simpleSlurm.maximumRetries=3",
      "pipelite.executor.simpleSlurm.timeout=10s",
      "pipelite.executor.simpleSlurm.logDir=TEST_DIR1",
      // AWS
      "pipelite.executor.awsBatch.region=TEST",
      "pipelite.executor.awsBatch.queue=TEST",
      "pipelite.executor.awsBatch.definition=TEST",
      "pipelite.executor.awsBatch.immediateRetries=2",
      "pipelite.executor.awsBatch.maximumRetries=3",
      "pipelite.executor.awsBatch.timeout=10s"
    })
@ActiveProfiles("test")
@DirtiesContext
public class ExecutorConfigurationTest {

  @Autowired ExecutorConfiguration config;

  @Test
  public void cmdProperties() {
    assertThat(config.getCmd().getUser()).isEqualTo("TEST");
    assertThat(config.getCmd().getHost()).isEqualTo("TEST");
    assertThat(config.getCmd().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getCmd().getMaximumRetries()).isEqualTo(3);
    assertThat(config.getCmd().getTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  public void lsfProperties() {
    assertThat(config.getSimpleLsf().getUser()).isEqualTo("TEST");
    assertThat(config.getSimpleLsf().getHost()).isEqualTo("TEST");
    assertThat(config.getSimpleLsf().getMemory()).isEqualTo(1);
    assertThat(config.getSimpleLsf().getCpu()).isEqualTo(1);
    assertThat(config.getSimpleLsf().getQueue()).isEqualTo("TEST");
    assertThat(config.getSimpleLsf().getMemoryTimeout().toMillis() / 1000L).isEqualTo(15);
    assertThat(config.getSimpleLsf().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getSimpleLsf().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getSimpleLsf().getMaximumRetries()).isEqualTo(3);
    assertThat(config.getSimpleLsf().getLogDir()).isEqualTo("TEST_DIR1");
    assertThat(config.getSimpleLsf().getTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  public void slurmProperties() {
    assertThat(config.getSimpleSlurm().getUser()).isEqualTo("TEST");
    assertThat(config.getSimpleSlurm().getHost()).isEqualTo("TEST");
    assertThat(config.getSimpleSlurm().getMemory()).isEqualTo(1);
    assertThat(config.getSimpleSlurm().getCpu()).isEqualTo(1);
    assertThat(config.getSimpleSlurm().getQueue()).isEqualTo("TEST");
    assertThat(config.getSimpleSlurm().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getSimpleSlurm().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getSimpleSlurm().getMaximumRetries()).isEqualTo(3);
    assertThat(config.getSimpleSlurm().getLogDir()).isEqualTo("TEST_DIR1");
    assertThat(config.getSimpleSlurm().getTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  public void awsBatchProperties() {
    assertThat(config.getAwsBatch().getRegion()).isEqualTo("TEST");
    assertThat(config.getAwsBatch().getQueue()).isEqualTo("TEST");
    assertThat(config.getAwsBatch().getDefinition()).isEqualTo("TEST");
    assertThat(config.getAwsBatch().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getAwsBatch().getMaximumRetries()).isEqualTo(3);
    assertThat(config.getAwsBatch().getTimeout()).isEqualTo(Duration.ofSeconds(10));
  }
}
