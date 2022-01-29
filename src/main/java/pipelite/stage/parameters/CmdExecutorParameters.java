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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.cmd.LogFileResolver;
import pipelite.stage.parameters.cmd.LogFileRetentionPolicy;
import pipelite.stage.parameters.cmd.WorkDirResolver;

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

  /**
   * The working directory where job definition files, output files or any other files required for
   * the job execution are written. The default value is pipelite. The following placeholders can be
   * used as part of the working directory: %PIPELINE% will be replaced by the pipeline name,
   * %PROCESS% will be replaced by the process id, %STAGE% will be replaced by the stage name.
   */
  private String workDir;

  /** The stage log file retention policy. */
  private LogFileRetentionPolicy logRetention;

  public void applyDefaults(ExecutorConfiguration executorConfiguration) {
    CmdExecutorParameters defaultParams = executorConfiguration.getCmd();
    if (defaultParams == null) {
      return;
    }
    super.applyDefaults(defaultParams);
    applyDefault(this::getHost, this::setHost, defaultParams::getHost);
    applyDefault(this::getUser, this::setUser, defaultParams::getUser);
    applyDefault(this::getWorkDir, this::setWorkDir, defaultParams::getWorkDir);
    applyDefault(this::getLogRetention, this::setLogRetention, defaultParams::getLogRetention);
    if (env == null) {
      env = new HashMap<>();
    }
    applyMapDefaults(env, defaultParams.env);
  }

  @Override
  public void validate() {
    super.validate();
    if (workDir != null) {
      ExecutorParametersValidator.validatePath(workDir, "workDir");
    }
  }

  /** Returns the working directory. */
  public static Path getWorkDir(StageExecutorRequest request, CmdExecutorParameters params) {
    String workDir = WorkDirResolver.resolve(request, params);
    return ExecutorParametersValidator.validatePath(workDir, "workDir");
  }

  /** Returns the stage log file. */
  public static Path getLogFile(StageExecutorRequest request, CmdExecutorParameters params) {
    String outputFile = LogFileResolver.resolve(request, params);
    return ExecutorParametersValidator.validatePath(outputFile, "logFile");
  }
}
