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
package pipelite.stage;

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
public class StageParameters implements ConfigurableStageParameters {

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
    setHost(StageParametersUtils.getHost(this, stageConfiguration));
    setTimeout(StageParametersUtils.getTimeout(this, stageConfiguration));
    setMaximumRetries(StageParametersUtils.getMaximumRetries(this, stageConfiguration));
    setImmediateRetries(StageParametersUtils.getImmediateRetries(this, stageConfiguration));
    setEnv(StageParametersUtils.getEnv(this, stageConfiguration));
    setWorkDir(StageParametersUtils.getTempDir(this, stageConfiguration));
    setMemory(StageParametersUtils.getMemory(this, stageConfiguration));
    setMemoryTimeout(StageParametersUtils.getMemoryTimeout(this, stageConfiguration));
    setCores(StageParametersUtils.getCores(this, stageConfiguration));
    setQueue(StageParametersUtils.getQueue(this, stageConfiguration));
    setSingularityImage(StageParametersUtils.getSingularityImage(this, stageConfiguration));
  }

  public String json() {
    return Json.serializeSafely(this);
  }
}
