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

import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.ExecutorConfiguration;

@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class CmdExecutorParameters extends ExecutorParameters {

  /** The remote host. */
  private String host;

  /** The environmental variables. */
  private Map<String, String> env;

  /** The working directory for stdout and stderr files and job definition files. */
  private String workDir;

  public void applyDefaults(ExecutorConfiguration executorConfiguration) {
    CmdExecutorParameters defaultParams = executorConfiguration.getCmd();
    super.applyDefaults(defaultParams);
    applyDefault(this::getHost, this::setHost, defaultParams::getHost);
    applyDefault(this::getWorkDir, this::setWorkDir, defaultParams::getWorkDir);
    applyMapDefaults(env, defaultParams.env);
  }
}
