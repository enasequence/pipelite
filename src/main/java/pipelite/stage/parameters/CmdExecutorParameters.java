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

import java.util.Map;

@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class CmdExecutorParameters extends ExecutorParameters {

  /** The remote host. */
  private String host;

  /** The singularity image name. */
  private String singularityImage;

  /** The environmental variables. */
  private Map<String, String> env;

  public void applyDefaults(StageConfiguration stageConfiguration) {
    CmdExecutorParameters defaultParams = stageConfiguration.getCmd();
    super.applyDefaults(defaultParams);
    applyDefault(this::getHost, this::setHost, defaultParams::getHost);
    applyDefault(
        this::getSingularityImage, this::setSingularityImage, defaultParams::getSingularityImage);
    if (defaultParams != null && defaultParams.env != null) {
      for (String key : defaultParams.env.keySet()) {
        if (!env.containsKey(key)) {
          env.put(key, defaultParams.env.get(key));
        }
      }
    }
  }
}
