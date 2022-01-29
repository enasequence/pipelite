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

import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.CmdExecutorParameters;

/**
 * Resolves the working directory. Defaults to 'pipelite'. Substitutes working directory
 * placeholders.
 */
public class WorkDirResolver {

  public static final String DEFAULT_WORKDIR = "pipelite";
  public static final String PIPELINE_PLACEHOLDER = "%PIPELINE%";
  public static final String PROCESS_PLACEHOLDER = "%PROCESS%";
  public static final String STAGE_PLACEHOLDER = "%STAGE%";

  private WorkDirResolver() {}

  /**
   * Resolves the working directory. Defaults to 'pipelite'. Substitutes working directory
   * placeholders: %PIPELINE% will become pipeline name, %PROCESS% will become the process id,
   * %STAGE% will become the stage name.
   */
  public static String resolve(StageExecutorRequest request, CmdExecutorParameters params) {
    String workDir =
        (params == null || params.getWorkDir() == null) ? DEFAULT_WORKDIR : params.getWorkDir();
    workDir = workDir.replace(PIPELINE_PLACEHOLDER, sanitize(request.getPipelineName()));
    workDir = workDir.replace(PROCESS_PLACEHOLDER, sanitize(request.getProcessId()));
    workDir = workDir.replace(STAGE_PLACEHOLDER, sanitize(request.getStage().getStageName()));
    return workDir;
  }
}
