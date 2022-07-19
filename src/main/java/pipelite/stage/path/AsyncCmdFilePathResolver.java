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

import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.AsyncCmdExecutorParameters;

import java.nio.file.Paths;

import static pipelite.stage.path.FilePathNormalizer.normalize;
import static pipelite.stage.path.FilePathSanitizer.sanitize;

/**
 * Resolves the log file path and name: <dir>/<user>/<pipeline>/<process>/<stage>.log where the
 * <dir> is defined by executor parameters.
 */
public abstract class AsyncCmdFilePathResolver {

  private final String fileSuffix;

  /** Submit user placeholder used by the LSF executor to create the log output directory. */
  private final String userPlaceholder;

  public AsyncCmdFilePathResolver(String fileSuffix, String userPlaceholder) {
    this.fileSuffix = fileSuffix;
    this.userPlaceholder = userPlaceholder;
  }

  /**
   * A file path that may contain placeholders to be substituted by the executor during job
   * submission.
   */
  public class PlaceholderPath {
    public String dir(StageExecutorRequest request) {
      return AsyncCmdFilePathResolver.this.dir(request, true);
    }

    public String file(StageExecutorRequest request) {
      return AsyncCmdFilePathResolver.this.file(request, true);
    }
  }

  /** A resolved file path without any placeholders. */
  public class ResolvedPath {
    public String dir(StageExecutorRequest request) {
      return AsyncCmdFilePathResolver.this.dir(request, false);
    }

    public String file(StageExecutorRequest request) {
      return AsyncCmdFilePathResolver.this.file(request, false);
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
    fileName.append(fileSuffix);
    return normalize(fileName.toString());
  }

  /** Returns the log file directory. */
  private String dir(StageExecutorRequest request, boolean isPlaceHolders) {
    AsyncCmdExecutorParameters params =
        (AsyncCmdExecutorParameters) request.getStage().getExecutor().getExecutorParams();

    return normalize(
        Paths.get(
                getLogDirFromParams(params),
                (isPlaceHolders && userPlaceholder != null && !userPlaceholder.isEmpty())
                    ? userPlaceholder
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
