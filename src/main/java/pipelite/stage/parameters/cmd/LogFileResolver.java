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
package pipelite.stage.parameters.cmd;

import static pipelite.stage.parameters.cmd.FilePathSanitizer.sanitize;

import java.nio.file.Path;
import java.nio.file.Paths;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.CmdExecutorParameters;

/** Resolves the stage log file written to the working directory. */
public class LogFileResolver {

  public static final String OUT_FILE_SUFFIX = ".out";

  private LogFileResolver() {}

  /** Resolves the stage log file written to the working directory. */
  public static String resolve(StageExecutorRequest request, CmdExecutorParameters params) {
    return resolveNoSuffix(request, params) + OUT_FILE_SUFFIX;
  }

  public static String resolveNoSuffix(StageExecutorRequest request, CmdExecutorParameters params) {
    String workDir = WorkDirResolver.resolve(request, params);
    Path outputFile =
        Paths.get(
            workDir,
            sanitize(request.getPipelineName())
                + "_"
                + sanitize(request.getProcessId())
                + "_"
                + sanitize(request.getStage().getStageName()));
    return outputFile.toString();
  }
}
