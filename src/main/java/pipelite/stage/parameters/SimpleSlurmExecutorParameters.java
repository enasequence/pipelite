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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.ExecutorConfiguration;

/**
 * Simple SLURM executor parameters assuming a single possible multithreaded program (--ntasks=1).
 * Only some SLURM options are available. For more information please refer to SLURM documentation.
 */
@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class SimpleSlurmExecutorParameters extends AbstractSlurmExecutorParameters {

  /** The SLURM queue name. */
  private String queue;

  /** The SLURM number of requested cpus (-cpus-per-task option). */
  private Integer cpu;

  /** The SLURM amount of requested memory (-mem option). */
  private Integer memory;

  /** The SLURM memory units (-mem option). Default is megabytes. */
  private String memoryUnits;

  @Override
  public void applyDefaults(ExecutorConfiguration executorConfiguration) {
    SimpleSlurmExecutorParameters defaultParams = executorConfiguration.getSimpleSlurm();
    if (defaultParams == null) {
      return;
    }
    super.applyDefaults(defaultParams);
    applyDefault(this::getQueue, this::setQueue, defaultParams::getQueue);
    applyDefault(this::getCpu, this::setCpu, defaultParams::getCpu);
    applyDefault(this::getMemory, this::setMemory, defaultParams::getMemory);
    applyDefault(this::getMemoryUnits, this::setMemoryUnits, defaultParams::getMemoryUnits);
  }
}
