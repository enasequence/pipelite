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
package pipelite.executor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.StageConfiguration;
import pipelite.json.Json;

@Data
@Builder
@Flogger
public class StageExecutorParameters implements ConfigurableStageExecutorParameters {

  private String host; // Executors: SSH
  private Duration timeout; // Executors: all
  private Integer maximumRetries; // Executors: all
  private Integer immediateRetries; // Executors: all
  @Builder.Default Map<String, String> env = new HashMap<>(); // Executors: all
  private String workDir; // Executors: LSF
  private Integer memory; // Executors: LSF
  private Duration memoryTimeout; // Executors: LSF
  private Integer cores; // Executors: LSF
  private String queue; // Executors: LSF
  private Duration pollFrequency; // Executors: LSF
  private String singularityImage;

  /* Add parameters. Existing values will not be replaced. */
  public void add(StageConfiguration stageConfiguration) {
    if (stageConfiguration == null) {
      return;
    }
    setHost(StageExecutorParametersUtils.getHost(this, stageConfiguration));
    setTimeout(StageExecutorParametersUtils.getTimeout(this, stageConfiguration));
    setMaximumRetries(StageExecutorParametersUtils.getMaximumRetries(this, stageConfiguration));
    setImmediateRetries(StageExecutorParametersUtils.getImmediateRetries(this, stageConfiguration));
    setEnv(StageExecutorParametersUtils.getEnv(this, stageConfiguration));
    setWorkDir(StageExecutorParametersUtils.getTempDir(this, stageConfiguration));
    setMemory(StageExecutorParametersUtils.getMemory(this, stageConfiguration));
    setMemoryTimeout(StageExecutorParametersUtils.getMemoryTimeout(this, stageConfiguration));
    setCores(StageExecutorParametersUtils.getCores(this, stageConfiguration));
    setQueue(StageExecutorParametersUtils.getQueue(this, stageConfiguration));
    setSingularityImage(StageExecutorParametersUtils.getSingularityImage(this, stageConfiguration));
  }

  public String serialize() {
    return Json.serialize(this);
  }

  @Override
  public String toString() {
    return serialize();
  }

  public static StageExecutorParameters deserialize(String json) {
    return Json.deserialize(json, StageExecutorParameters.class);
  }
}
