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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.*;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.ExecutorConfiguration;

@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class CmdExecutorParameters extends ExecutorParameters {

  public static final int DEFAULT_LOG_BYTES = 1024 * 1024;

  /** The remote host. */
  private String host;

  /** The user used to connect to the remote host. */
  private String user;

  /** The environmental variables. */
  private Map<String, String> env;

  /** The working directory where the output file and job definition files are written. */
  private String workDir;

  /** The permanent error exit codes. Permanent errors are never retried. */
  @Singular private List<Integer> permanentErrors;

  /** The number of last bytes from the output file saved in the stage log. */
  @Builder.Default private int logBytes = DEFAULT_LOG_BYTES;

  public void applyDefaults(ExecutorConfiguration executorConfiguration) {
    CmdExecutorParameters defaultParams = executorConfiguration.getCmd();
    if (defaultParams == null) {
      return;
    }
    super.applyDefaults(defaultParams);
    applyDefault(this::getHost, this::setHost, defaultParams::getHost);
    applyDefault(this::getUser, this::setUser, defaultParams::getUser);
    applyDefault(this::getWorkDir, this::setWorkDir, defaultParams::getWorkDir);
    applyDefault(this::getLogBytes, this::setLogBytes, defaultParams::getLogBytes);
    if (env == null) {
      env = new HashMap<>();
    }
    applyMapDefaults(env, defaultParams.env);
    if (permanentErrors == null) {
      permanentErrors = new ArrayList<>();
    }
    applyListDefaults(permanentErrors, defaultParams.permanentErrors);
  }

  @Override
  public void validate() {
    super.validate();
    if (workDir != null) {
      validatePath(workDir, "workDir");
    }
  }
}
