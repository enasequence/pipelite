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

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.StageConfiguration;

@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class AwsBatchExecutorParameters extends ExecutorParameters {

  /** The optional region name. */
  private String region;

  /** The queue name. */
  private String queue;

  /** The job definition name, name:revision, or ARN. */
  private String jobDefinition;

  /**
   * The job parameters. These parameters replace parameter substitution placeholders and override
   * corresponding parameter defaults in the job definition.
   */
  private Map<String, String> jobParameters;

  @Override
  public void applyDefaults(StageConfiguration stageConfiguration) {
    AwsBatchExecutorParameters defaultParams = stageConfiguration.getAwsBatch();
    super.applyDefaults(defaultParams);
    applyDefault(this::getRegion, this::setRegion, defaultParams::getRegion);
    applyDefault(this::getQueue, this::setQueue, defaultParams::getQueue);
    applyDefault(this::getJobDefinition, this::setJobDefinition, defaultParams::getJobDefinition);
    if (jobParameters == null) {
      jobParameters = new HashMap<>();
    }
    if (defaultParams != null && defaultParams.jobParameters != null) {
      for (String key : defaultParams.jobParameters.keySet()) {
        if (!jobParameters.containsKey(key)) {
          jobParameters.put(key, defaultParams.jobParameters.get(key));
        }
      }
    }
  }
}
