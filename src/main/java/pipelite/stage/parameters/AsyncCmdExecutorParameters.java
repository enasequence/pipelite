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

/** Asynchronous polling executor parameters. */
@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public abstract class AsyncCmdExecutorParameters extends CmdExecutorParameters {

  public static final Duration DEFAULT_LOG_TIMEOUT = Duration.ofSeconds(10);

  /**
   * The directory where stage log files are written: <logDir>/<user>/<pipeline>/<process>. The
   * <logDir> must exist on the LSF cluster.
   */
  private String logDir;

  /** The maximum wait time for the stage log file to become available. */
  private Duration logTimeout;

  public Duration getLogTimeout() {
    return logTimeout == null ? DEFAULT_LOG_TIMEOUT : logTimeout;
  }

  /**
   * Call to apply default values from stage configuration file.
   *
   * @param defaultParams executor parameters from configuration file
   */
  public void applyAsyncCmdExecutorDefaults(AsyncCmdExecutorParameters defaultParams) {
    if (defaultParams == null) {
      return;
    }
    applyCmdExecutorDefaults(defaultParams);
    if (logDir == null) setLogDir(defaultParams.getLogDir());
    if (logTimeout == null) setLogTimeout(defaultParams.getLogTimeout());
  }

  @Override
  public void validate() {
    super.validate();
    if (logDir != null) {
      ExecutorParametersValidator.validatePath(logDir, "logDir");
    }
  }

  // Json deserialization backwards compatibility for workDir field.
  public void setWorkDir(String workDir) {
    if (workDir != null && !workDir.isEmpty()) {
      this.logDir = workDir;
    }
  }
}
