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

import java.time.Duration;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.path.LsfFilePathResolver;
import pipelite.stage.path.LsfLogFilePathResolver;

/** Shared LSF executor parameters. */
@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class AbstractLsfExecutorParameters extends CmdExecutorParameters {

  public static final Duration DEFAULT_LOG_TIMEOUT = Duration.ofSeconds(10);

  /**
   * The directory where stage log files are written: <logDir>/<user>/<pipeline>/<process>. The
   * <logDir> must exist on the LSF cluster. Default value: pipelite.
   */
  private String logDir;

  /** The maximum wait time for the stage log file to become available. */
  @Builder.Default private Duration logTimeout = DEFAULT_LOG_TIMEOUT;

  /**
   * Call to apply default values from stage configuration.
   *
   * @param params executor parameters extracted from stage configuration
   */
  protected void applyDefaults(AbstractLsfExecutorParameters params) {
    if (params == null) {
      return;
    }
    applyDefault(this::getLogDir, this::setLogDir, params::getLogDir);
    applyDefault(this::getLogTimeout, this::setLogTimeout, params::getLogTimeout);
  }

  @Override
  public void validate() {
    super.validate();
    if (logDir != null) {
      ExecutorParametersValidator.validatePath(logDir, "logDir");
    }
  }

  public String resolveLogDir(StageExecutorRequest request, LsfFilePathResolver.Format format) {
    return ExecutorParametersValidator.validatePath(
            new LsfLogFilePathResolver(request, this).getDir(format), "logDir")
        .toString();
  }

  public String resolveLogFileName(StageExecutorRequest request) {
    return ExecutorParametersValidator.validatePath(
            new LsfLogFilePathResolver(request, this).getFileName(), "logFileName")
        .toString();
  }

  public String resolveLogFile(StageExecutorRequest request, LsfFilePathResolver.Format format) {
    return ExecutorParametersValidator.validatePath(
            new LsfLogFilePathResolver(request, this).getFile(format), "logFile")
        .toString();
  }
}