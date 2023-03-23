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

import java.time.Duration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.ExecutorConfiguration;

/**
 * Simple LSF executor parameters. Only some LSF options are available. For more information please
 * refer to LSF documentation.
 */
@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class SimpleLsfExecutorParameters extends AbstractLsfExecutorParameters {

  /** The LSF queue name. */
  private String queue;

  /** The LSF number of requested cpus (-n option). */
  private Integer cpu;

  /** The LSF amount of requested memory (-M and -R rusage[mem=] option). */
  private Integer memory;

  /** The LSF memory units (-M and -R rusage[mem=] option). */
  private String memoryUnits;

  /** The LSF memory duration (-R rusage[mem=:duration=] option). */
  private Duration memoryTimeout;

  /** The LSF job group name. */
  private String jobGroup;

  @Override
  public void applyDefaults(ExecutorConfiguration executorConfiguration) {
    SimpleLsfExecutorParameters defaultParams = executorConfiguration.getSimpleLsf();
    if (defaultParams == null) {
      return;
    }
    applyAsyncCmdExecutorDefaults(defaultParams);
    if (queue == null) setQueue(defaultParams.getQueue());
    if (cpu == null) setCpu(defaultParams.getCpu());
    if (memory == null) setMemory(defaultParams.getMemory());
    if (memoryUnits == null) setMemoryUnits(defaultParams.getMemoryUnits());
    if (memoryTimeout == null) setMemoryTimeout(defaultParams.getMemoryTimeout());
    if (jobGroup == null) setJobGroup(defaultParams.getJobGroup());
  }
}
