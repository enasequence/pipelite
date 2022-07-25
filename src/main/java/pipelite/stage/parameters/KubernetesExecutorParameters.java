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
package pipelite.stage.parameters;

import io.fabric8.kubernetes.api.model.Quantity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.ExecutorConfiguration;

@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class KubernetesExecutorParameters extends ExecutorParameters {

  /** The Kubernetes context. */
  private String context;

  /** The Kubernetes namespace. */
  private String namespace;

  /** The Job cpu request. */
  private Quantity cpu;

  /** The Job memory request. */
  private Quantity memory;

  /** The Job cpu limit. */
  private Quantity cpuLimit;

  /** The Job memory limit. */
  private Quantity memoryLimit;

  @Override
  public void applyDefaults(ExecutorConfiguration executorConfiguration) {
    KubernetesExecutorParameters defaultParams = executorConfiguration.getKubernetes();
    if (defaultParams == null) {
      return;
    }
    super.applyDefaults(defaultParams);
    applyDefault(this::getContext, this::setContext, defaultParams::getContext);
    applyDefault(this::getNamespace, this::setNamespace, defaultParams::getNamespace);
    applyDefault(this::getCpu, this::setCpu, defaultParams::getCpu);
    applyDefault(this::getMemory, this::setMemory, defaultParams::getMemory);
    applyDefault(this::getCpuLimit, this::setCpuLimit, defaultParams::getCpuLimit);
    applyDefault(this::getMemoryLimit, this::setMemoryLimit, defaultParams::getMemoryLimit);
  }

  @Override
  public void validate() {
    super.validate();
  }
}
