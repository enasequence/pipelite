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
package pipelite.stage.path;

import static pipelite.stage.path.FilePathNormalizer.normalize;
import static pipelite.stage.path.FilePathSanitizer.sanitize;

import java.nio.file.Paths;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.AsyncCmdExecutorParameters;

/**
 * Resolves the log file path and name: <dir>/<user>/<pipeline>/<process>/<stage>.log where the
 * <dir> is defined by executor parameters.
 */
public abstract class LogFilePathResolver {

  private static final String FILE_SUFFIX = ".out";

  /** Submit user placeholder used by the LSF executor to create the log output directory. */
  private final String submitUserPlaceholder;

  public LogFilePathResolver(String submitUserPlaceholder) {
    this.submitUserPlaceholder = submitUserPlaceholder;
  }

  /**
   * A file path that may contain placeholders to be substituted by the executor during job
   * submission.
   */
  public class PlaceholderPath {
    public String dir(StageExecutorRequest request) {
      return LogFilePathResolver.this.dir(request, true);
    }

    public String file(StageExecutorRequest request) {
      return LogFilePathResolver.this.file(request, true);
    }
  }

  /** A resolved file path without any placeholders. */
  public class ResolvedPath {
    public String dir(StageExecutorRequest request) {
      return LogFilePathResolver.this.dir(request, false);
    }

    public String file(StageExecutorRequest request) {
      return LogFilePathResolver.this.file(request, false);
    }
  }

  private final PlaceholderPath placeholderPath = new PlaceholderPath();
  private final ResolvedPath resolvedPath = new ResolvedPath();

  /**
   * A file path that may contain placeholders to be substituted by the executor during job
   * submission.
   */
  public PlaceholderPath placeholderPath() {
    return placeholderPath;
  }

  /** A resolved file path without any placeholders. */
  public ResolvedPath resolvedPath() {
    return resolvedPath;
  }

  private String getLogDirFromParams(AsyncCmdExecutorParameters params) {
    return (params == null || params.getLogDir() == null) ? "" : params.getLogDir();
  }

  private String getUserFromParams(AsyncCmdExecutorParameters params) {
    return params.resolveUser();
  }

  /** Returns the log file name. */
  public String fileName(StageExecutorRequest request) {
    StringBuilder fileName = new StringBuilder();
    fileName.append(sanitize(request.getStage().getStageName()));
    fileName.append(FILE_SUFFIX);
    return normalize(fileName.toString());
  }

  /** Returns the log file directory. */
  private String dir(StageExecutorRequest request, boolean isPlaceHolders) {
    AsyncCmdExecutorParameters params =
        (AsyncCmdExecutorParameters) request.getStage().getExecutor().getExecutorParams();

    return normalize(
        Paths.get(
                getLogDirFromParams(params),
                (isPlaceHolders
                        && submitUserPlaceholder != null
                        && !submitUserPlaceholder.isEmpty())
                    ? submitUserPlaceholder
                    : getUserFromParams(params),
                sanitize(request.getPipelineName()),
                sanitize(request.getProcessId()))
            .toString());
  }

  /** Returns the log file full path. */
  private String file(StageExecutorRequest request, boolean isPlaceHolders) {
    return normalize(Paths.get(dir(request, isPlaceHolders), fileName(request)).toString());
  }
}
