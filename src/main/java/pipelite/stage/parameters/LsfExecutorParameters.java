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
package pipelite.stage.parameters;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.StageConfiguration;

import java.time.Duration;

@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper=true)
public class LsfExecutorParameters extends CmdExecutorParameters {

  /** The queue name. */
  private String queue;

  /** The number of requested cores. */
  private Integer cores;

  /** The amount of requested memory in MBytes. */
  private Integer memory;

  private Duration memoryTimeout;

  /** The working directory. */
  private String workDir;

  @Override
  public void applyDefaults(StageConfiguration stageConfiguration) {
    LsfExecutorParameters defaultParams = stageConfiguration.getLsf();
    super.applyDefaults(defaultParams);
    applyDefault(this::getQueue, this::setQueue, defaultParams::getQueue);
    applyDefault(this::getCores, this::setCores, defaultParams::getCores);
    applyDefault(this::getMemory, this::setMemory, defaultParams::getMemory);
    applyDefault(this::getMemoryTimeout, this::setMemoryTimeout, defaultParams::getMemoryTimeout);
    applyDefault(this::getWorkDir, this::setWorkDir, defaultParams::getWorkDir);
  }
}
