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

import static pipelite.stage.path.FilePathSanitizer.sanitize;

import java.nio.file.Paths;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.AbstractLsfExecutorParameters;

/**
 * Resolves the lsf directory <dir>/"%U"/<pipeline>/<process> or <dir>/<user>/<pipeline>/<process>.
 */
public class LsfFilePathResolver {

  public enum Format {
    WITH_LSF_PATTERN,
    WITHOUT_LSF_PATTERN
  }

  private final StageExecutorRequest request;
  private final AbstractLsfExecutorParameters params;
  private final String dir;
  private final String suffix;

  public LsfFilePathResolver(
      StageExecutorRequest request,
      AbstractLsfExecutorParameters params,
      String dir,
      String suffix) {
    this.request = request;
    this.params = params;
    this.dir = dir;
    this.suffix = suffix;
  }

  public String getDir(Format format) {
    return Paths.get(
            dir,
            (Format.WITH_LSF_PATTERN == format) ? "%U" : params.resolveUser(),
            sanitize(request.getPipelineName()),
            sanitize(request.getProcessId()))
        .toString();
  }

  public String getFileName() {
    StringBuilder fileName = new StringBuilder();
    fileName.append(sanitize(request.getStage().getStageName()));
    fileName.append(suffix);
    return fileName.toString();
  }

  public String getFile(Format format) {
    return Paths.get(getDir(format), getFileName()).toString();
  }
}
