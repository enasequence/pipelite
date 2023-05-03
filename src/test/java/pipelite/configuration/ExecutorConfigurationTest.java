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

import static org.assertj.core.api.Assertions.assertThat;

import io.fabric8.kubernetes.api.model.Quantity;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import pipelite.stage.parameters.*;
import pipelite.test.configuration.PipeliteTestConfigWithConfigurations;

@SpringBootTest(
    classes = PipeliteTestConfigWithConfigurations.class,
    properties = {
      // CMD
      "pipelite.executor.cmd.user=USER", // CmdExecutorParameters
      "pipelite.executor.cmd.host=HOST", // CmdExecutorParameters
      "pipelite.executor.cmd.immediateRetries=2", // ExecutorParameters
      "pipelite.executor.cmd.maximumRetries=2", // ExecutorParameters
      "pipelite.executor.cmd.timeout=10s", // ExecutorParameters
      "pipelite.executor.cmd.permanentErrors=10,11", // ExecutorParameters
      // LSF
      "pipelite.executor.simpleLsf.memory=1", // SimpleLsfExecutorParameters
      "pipelite.executor.simpleLsf.cpu=1", // SimpleLsfExecutorParameters
      "pipelite.executor.simpleLsf.queue=QUEUE", // SimpleLsfExecutorParameters
      "pipelite.executor.simpleLsf.memoryTimeout=15s", // SimpleLsfExecutorParameters
      "pipelite.executor.simpleLsf.logDir=LOG_DIR", // AsyncCmdExecutorParameters
      "pipelite.executor.simpleLsf.logTimeout=1m", // AsyncCmdExecutorParameters
      "pipelite.executor.simpleLsf.user=USER", // CmdExecutorParameters
      "pipelite.executor.simpleLsf.host=HOST", // CmdExecutorParameters
      "pipelite.executor.simpleLsf.immediateRetries=2", // ExecutorParameters
      "pipelite.executor.simpleLsf.maximumRetries=2", // ExecutorParameters
      "pipelite.executor.simpleLsf.timeout=10s", // ExecutorParameters
      "pipelite.executor.simpleLsf.permanentErrors=10,11", // ExecutorParameters
      // SLURM
      "pipelite.executor.simpleSlurm.memory=1", // SimpleSlurmExecutorParameters
      "pipelite.executor.simpleSlurm.cpu=1", // SimpleSlurmExecutorParameters
      "pipelite.executor.simpleSlurm.account=ACCOUNT", // SimpleSlurmExecutorParameters
      "pipelite.executor.simpleSlurm.queue=QUEUE", // SimpleSlurmExecutorParameters
      "pipelite.executor.simpleSlurm.memoryTimeout=15s", // SimpleSlurmExecutorParameters
      "pipelite.executor.simpleSlurm.logDir=LOG_DIR", // AsyncCmdExecutorParameters
      "pipelite.executor.simpleSlurm.logTimeout=1m", // AsyncCmdExecutorParameters
      "pipelite.executor.simpleSlurm.user=USER", // CmdExecutorParameters
      "pipelite.executor.simpleSlurm.host=HOST", // CmdExecutorParameters
      "pipelite.executor.simpleSlurm.immediateRetries=2", // ExecutorParameters
      "pipelite.executor.simpleSlurm.maximumRetries=2", // ExecutorParameters
      "pipelite.executor.simpleSlurm.timeout=10s", // ExecutorParameters
      "pipelite.executor.simpleSlurm.permanentErrors=10,11", // ExecutorParameters
      // KUBERNETES
      "pipelite.executor.kubernetes.context=CONTEXT",
      "pipelite.executor.kubernetes.namespace=NAMESPACE",
      "pipelite.executor.kubernetes.cpu=1",
      "pipelite.executor.kubernetes.memory=1",
      "pipelite.executor.kubernetes.cpuLimit=1",
      "pipelite.executor.kubernetes.memoryLimit=1",
      "pipelite.executor.kubernetes.immediateRetries=2", // ExecutorParameters
      "pipelite.executor.kubernetes.maximumRetries=2", // ExecutorParameters
      "pipelite.executor.kubernetes.timeout=10s", // ExecutorParameters
      "pipelite.executor.kubernetes.permanentErrors=10,11", // ExecutorParameters
      // AWS
      "pipelite.executor.awsBatch.region=REGION",
      "pipelite.executor.awsBatch.queue=QUEUE",
      "pipelite.executor.awsBatch.definition=DEFINITION",
      "pipelite.executor.awsBatch.immediateRetries=2", // ExecutorParameters
      "pipelite.executor.awsBatch.maximumRetries=2", // ExecutorParameters
      "pipelite.executor.awsBatch.timeout=10s", // ExecutorParameters
      "pipelite.executor.awsBatch.permanentErrors=10,11", // ExecutorParameters
    })
@DirtiesContext
public class ExecutorConfigurationTest {

  @Autowired ExecutorConfiguration config;

  @Test
  public void cmdProperties() {
    assertThat(config.getCmd().getUser()).isEqualTo("USER");
    assertThat(config.getCmd().getHost()).isEqualTo("HOST");

    assertThat(config.getCmd().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getCmd().getMaximumRetries()).isEqualTo(2);
    assertThat(config.getCmd().getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.getCmd().getPermanentErrors()).containsExactly(10, 11);

    // Test defaults overwrite nulls

    CmdExecutorParameters params = new CmdExecutorParameters();

    assertThat(params.getUser()).isNull();
    assertThat(params.getHost()).isNull();

    assertThat(params.getImmediateRetries())
        .isEqualTo(CmdExecutorParameters.DEFAULT_IMMEDIATE_RETRIES);
    assertThat(params.getMaximumRetries()).isEqualTo(CmdExecutorParameters.DEFAULT_MAX_RETRIES);
    assertThat(params.getTimeout()).isEqualTo(CmdExecutorParameters.DEFAULT_TIMEOUT);
    assertThat(params.getPermanentErrors()).isNull();

    params.applyDefaults(config);

    assertThat(params.getUser()).isEqualTo("USER");
    assertThat(params.getHost()).isEqualTo("HOST");

    assertThat(params.getImmediateRetries()).isEqualTo(2);
    assertThat(params.getMaximumRetries()).isEqualTo(2);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(params.getPermanentErrors()).containsExactly(10, 11);

    // Test defaults do not overwrite not nulls

    params = new CmdExecutorParameters();
    params.setImmediateRetries(100);
    params.setMaximumRetries(100);
    params.setTimeout(Duration.ofSeconds(100));

    params.applyDefaults(config);

    assertThat(params.getImmediateRetries()).isEqualTo(100);
    assertThat(params.getMaximumRetries()).isEqualTo(100);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(100));
  }

  @Test
  public void lsfProperties() {
    assertThat(config.getSimpleLsf().getMemory()).isEqualTo(1);
    assertThat(config.getSimpleLsf().getCpu()).isEqualTo(1);
    assertThat(config.getSimpleLsf().getQueue()).isEqualTo("QUEUE");
    assertThat(config.getSimpleLsf().getMemoryTimeout().toMillis() / 1000L).isEqualTo(15);
    assertThat(config.getSimpleLsf().getLogDir()).isEqualTo("LOG_DIR");
    assertThat(config.getSimpleLsf().getLogTimeout()).isEqualTo(Duration.ofMinutes(1));

    assertThat(config.getSimpleLsf().getUser()).isEqualTo("USER");
    assertThat(config.getSimpleLsf().getHost()).isEqualTo("HOST");

    assertThat(config.getSimpleLsf().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getSimpleLsf().getMaximumRetries()).isEqualTo(2);
    assertThat(config.getSimpleLsf().getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.getSimpleLsf().getPermanentErrors()).containsExactly(10, 11);

    // Test defaults overwrite nulls

    SimpleLsfExecutorParameters params = new SimpleLsfExecutorParameters();

    assertThat(params.getMemory()).isNull();
    assertThat(params.getCpu()).isNull();
    assertThat(params.getQueue()).isNull();
    assertThat(params.getMemoryTimeout()).isNull();
    assertThat(params.getLogDir()).isNull();
    assertThat(params.getLogTimeout()).isEqualTo(AsyncCmdExecutorParameters.DEFAULT_LOG_TIMEOUT);

    assertThat(params.getImmediateRetries())
        .isEqualTo(CmdExecutorParameters.DEFAULT_IMMEDIATE_RETRIES);
    assertThat(params.getMaximumRetries()).isEqualTo(CmdExecutorParameters.DEFAULT_MAX_RETRIES);
    assertThat(params.getTimeout()).isEqualTo(CmdExecutorParameters.DEFAULT_TIMEOUT);
    assertThat(params.getPermanentErrors()).isNull();

    params.applyDefaults(config);

    assertThat(params.getMemory()).isEqualTo(1);
    assertThat(params.getCpu()).isEqualTo(1);
    assertThat(params.getQueue()).isEqualTo("QUEUE");
    assertThat(params.getMemoryTimeout().toMillis() / 1000L).isEqualTo(15);
    assertThat(params.getLogDir()).isEqualTo("LOG_DIR");
    assertThat(params.getLogTimeout()).isEqualTo(Duration.ofMinutes(1));

    assertThat(params.getImmediateRetries()).isEqualTo(2);
    assertThat(params.getMaximumRetries()).isEqualTo(2);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(params.getPermanentErrors()).containsExactly(10, 11);

    // Test defaults do not overwrite not nulls

    params = new SimpleLsfExecutorParameters();
    params.setLogTimeout(Duration.ofSeconds(100));
    params.setImmediateRetries(100);
    params.setMaximumRetries(100);
    params.setTimeout(Duration.ofSeconds(100));

    params.applyDefaults(config);

    assertThat(params.getLogTimeout()).isEqualTo(Duration.ofSeconds(100));
    assertThat(params.getImmediateRetries()).isEqualTo(100);
    assertThat(params.getMaximumRetries()).isEqualTo(100);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(100));
  }

  @Test
  public void slurmProperties() {
    assertThat(config.getSimpleSlurm().getMemory()).isEqualTo(1);
    assertThat(config.getSimpleSlurm().getCpu()).isEqualTo(1);
    assertThat(config.getSimpleSlurm().getAccount()).isEqualTo("ACCOUNT");
    assertThat(config.getSimpleSlurm().getQueue()).isEqualTo("QUEUE");
    assertThat(config.getSimpleSlurm().getLogDir()).isEqualTo("LOG_DIR");
    assertThat(config.getSimpleSlurm().getLogTimeout()).isEqualTo(Duration.ofMinutes(1));

    assertThat(config.getSimpleSlurm().getUser()).isEqualTo("USER");
    assertThat(config.getSimpleSlurm().getHost()).isEqualTo("HOST");

    assertThat(config.getSimpleSlurm().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getSimpleSlurm().getMaximumRetries()).isEqualTo(2);
    assertThat(config.getSimpleSlurm().getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.getSimpleSlurm().getPermanentErrors()).containsExactly(10, 11);

    // Test defaults overwrite nulls

    SimpleSlurmExecutorParameters params = new SimpleSlurmExecutorParameters();

    assertThat(params.getMemory()).isNull();
    assertThat(params.getCpu()).isNull();
    assertThat(params.getAccount()).isNull();
    assertThat(params.getQueue()).isNull();
    assertThat(params.getLogDir()).isNull();
    assertThat(params.getLogTimeout()).isEqualTo(AsyncCmdExecutorParameters.DEFAULT_LOG_TIMEOUT);

    assertThat(params.getImmediateRetries())
        .isEqualTo(CmdExecutorParameters.DEFAULT_IMMEDIATE_RETRIES);
    assertThat(params.getMaximumRetries()).isEqualTo(CmdExecutorParameters.DEFAULT_MAX_RETRIES);
    assertThat(params.getTimeout()).isEqualTo(CmdExecutorParameters.DEFAULT_TIMEOUT);
    assertThat(params.getPermanentErrors()).isNull();

    params.applyDefaults(config);

    assertThat(params.getMemory()).isEqualTo(1);
    assertThat(params.getCpu()).isEqualTo(1);
    assertThat(params.getAccount()).isEqualTo("ACCOUNT");
    assertThat(params.getQueue()).isEqualTo("QUEUE");
    assertThat(params.getLogDir()).isEqualTo("LOG_DIR");
    assertThat(params.getLogTimeout()).isEqualTo(Duration.ofMinutes(1));

    assertThat(params.getImmediateRetries()).isEqualTo(2);
    assertThat(params.getMaximumRetries()).isEqualTo(2);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(params.getPermanentErrors()).containsExactly(10, 11);

    // Test defaults do not overwrite not nulls

    params = new SimpleSlurmExecutorParameters();
    params.setLogTimeout(Duration.ofSeconds(100));
    params.setImmediateRetries(100);
    params.setMaximumRetries(100);
    params.setTimeout(Duration.ofSeconds(100));

    params.applyDefaults(config);

    assertThat(params.getLogTimeout()).isEqualTo(Duration.ofSeconds(100));
    assertThat(params.getImmediateRetries()).isEqualTo(100);
    assertThat(params.getMaximumRetries()).isEqualTo(100);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(100));
  }

  @Test
  public void kubernetesProperties() {
    assertThat(config.getKubernetes().getContext()).isEqualTo("CONTEXT");
    assertThat(config.getKubernetes().getNamespace()).isEqualTo("NAMESPACE");
    assertThat(config.getKubernetes().getCpu()).isEqualTo(new Quantity("1"));
    assertThat(config.getKubernetes().getMemory()).isEqualTo(new Quantity("1"));
    assertThat(config.getKubernetes().getCpuLimit()).isEqualTo(new Quantity("1"));
    assertThat(config.getKubernetes().getMemoryLimit()).isEqualTo(new Quantity("1"));

    assertThat(config.getKubernetes().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getKubernetes().getMaximumRetries()).isEqualTo(2);
    assertThat(config.getKubernetes().getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.getSimpleSlurm().getPermanentErrors()).containsExactly(10, 11);

    // Test defaults overwrite nulls

    KubernetesExecutorParameters params = new KubernetesExecutorParameters();

    assertThat(params.getContext()).isNull();
    assertThat(params.getNamespace()).isNull();
    assertThat(params.getCpu()).isNull();
    assertThat(params.getMemory()).isNull();
    assertThat(params.getCpuLimit()).isNull();
    assertThat(params.getMemoryLimit()).isNull();

    assertThat(params.getImmediateRetries())
        .isEqualTo(CmdExecutorParameters.DEFAULT_IMMEDIATE_RETRIES);
    assertThat(params.getMaximumRetries()).isEqualTo(CmdExecutorParameters.DEFAULT_MAX_RETRIES);
    assertThat(params.getTimeout()).isEqualTo(CmdExecutorParameters.DEFAULT_TIMEOUT);
    assertThat(params.getPermanentErrors()).isNull();

    params.applyDefaults(config);

    assertThat(params.getContext()).isEqualTo("CONTEXT");
    assertThat(params.getNamespace()).isEqualTo("NAMESPACE");
    assertThat(params.getCpu()).isEqualTo(new Quantity("1"));
    assertThat(params.getMemory()).isEqualTo(new Quantity("1"));
    assertThat(params.getCpuLimit()).isEqualTo(new Quantity("1"));
    assertThat(params.getMemoryLimit()).isEqualTo(new Quantity("1"));

    assertThat(params.getImmediateRetries()).isEqualTo(2);
    assertThat(params.getMaximumRetries()).isEqualTo(2);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(params.getPermanentErrors()).containsExactly(10, 11);

    // Test defaults do not overwrite not nulls

    params = new KubernetesExecutorParameters();
    params.setImmediateRetries(100);
    params.setMaximumRetries(100);
    params.setTimeout(Duration.ofSeconds(100));

    params.applyDefaults(config);

    assertThat(params.getImmediateRetries()).isEqualTo(100);
    assertThat(params.getMaximumRetries()).isEqualTo(100);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(100));
  }

  @Test
  public void awsBatchProperties() {
    assertThat(config.getAwsBatch().getRegion()).isEqualTo("REGION");
    assertThat(config.getAwsBatch().getQueue()).isEqualTo("QUEUE");
    assertThat(config.getAwsBatch().getDefinition()).isEqualTo("DEFINITION");

    assertThat(config.getAwsBatch().getImmediateRetries()).isEqualTo(2);
    assertThat(config.getAwsBatch().getMaximumRetries()).isEqualTo(2);
    assertThat(config.getAwsBatch().getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.getSimpleSlurm().getPermanentErrors()).containsExactly(10, 11);

    // Test defaults overwrite nulls

    AwsBatchExecutorParameters params = new AwsBatchExecutorParameters();

    assertThat(params.getRegion()).isNull();
    assertThat(params.getQueue()).isNull();
    assertThat(params.getDefinition()).isNull();

    assertThat(params.getImmediateRetries())
        .isEqualTo(CmdExecutorParameters.DEFAULT_IMMEDIATE_RETRIES);
    assertThat(params.getMaximumRetries()).isEqualTo(CmdExecutorParameters.DEFAULT_MAX_RETRIES);
    assertThat(params.getTimeout()).isEqualTo(CmdExecutorParameters.DEFAULT_TIMEOUT);
    assertThat(params.getPermanentErrors()).isNull();

    params.applyDefaults(config);

    assertThat(params.getRegion()).isEqualTo("REGION");
    assertThat(params.getQueue()).isEqualTo("QUEUE");
    assertThat(params.getDefinition()).isEqualTo("DEFINITION");

    assertThat(params.getImmediateRetries()).isEqualTo(2);
    assertThat(params.getMaximumRetries()).isEqualTo(2);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(params.getPermanentErrors()).containsExactly(10, 11);

    // Test defaults do not overwrite not nulls

    params = new AwsBatchExecutorParameters();
    params.setImmediateRetries(100);
    params.setMaximumRetries(100);
    params.setTimeout(Duration.ofSeconds(100));

    params.applyDefaults(config);

    assertThat(params.getImmediateRetries()).isEqualTo(100);
    assertThat(params.getMaximumRetries()).isEqualTo(100);
    assertThat(params.getTimeout()).isEqualTo(Duration.ofSeconds(100));
  }
}
