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

@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class CmdExecutorParameters extends ExecutorParameters {

  /** The remote host. */
  private String host;

  /** The user used to connect to the remote host. */
  private String user;

  /** The environmental variables. */
  private Map<String, String> env;

  public void applyDefaults(ExecutorConfiguration executorConfiguration) {
    CmdExecutorParameters defaultParams = executorConfiguration.getCmd();
    applyCmdExecutorDefaults(defaultParams);
  }

  /**
   * Call to apply default values from stage configuration file.
   *
   * @param defaultParams executor parameters from configuration file
   */
  public void applyCmdExecutorDefaults(CmdExecutorParameters defaultParams) {
    if (defaultParams == null) {
      return;
    }
    applyExecutorDefaults(defaultParams);
    if (host == null) setHost(defaultParams.getHost());
    if (user == null) setUser(defaultParams.getUser());
    if (env == null) {
      env = new HashMap<>();
    }
    applyMapDefaults(env, defaultParams.env);
  }

  public String resolveUser() {
    return user != null && !user.isEmpty() ? user : System.getProperty("user.name");
  }
}
