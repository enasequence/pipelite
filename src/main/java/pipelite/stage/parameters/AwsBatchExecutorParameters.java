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

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.ExecutorConfiguration;

/** AWDBatch executor parameters. For more information please refer to AWSBatch documentation. */
@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class AwsBatchExecutorParameters extends ExecutorParameters {

  /** The AWS region name. */
  private String region;

  /** The AWSBatch job queue. */
  private String queue;

  /** The AWSBatch job definition name, name:revision or ARN. */
  private String definition;

  /** The AWSBatch job definition parameters applied to the job definition by AWSBatch. */
  private Map<String, String> parameters;

  @Override
  public void applyDefaults(ExecutorConfiguration executorConfiguration) {
    AwsBatchExecutorParameters defaultParams = executorConfiguration.getAwsBatch();
    if (defaultParams == null) {
      return;
    }
    super.applyDefaults(defaultParams);
    applyDefault(this::getRegion, this::setRegion, defaultParams::getRegion);
    applyDefault(this::getQueue, this::setQueue, defaultParams::getQueue);
    applyDefault(this::getDefinition, this::setDefinition, defaultParams::getDefinition);
    if (parameters == null) {
      parameters = new HashMap<>();
    }
    applyMapDefaults(parameters, defaultParams.parameters);
  }

  @Override
  public void validate() {
    super.validate();
    ExecutorParametersValidator.validateNotNull(queue, "queue");
    ExecutorParametersValidator.validateNotNull(definition, "definition");
  }
}
