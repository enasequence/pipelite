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
import pipelite.stage.parameters.LsfExecutorParameters;

/**
 * Resolves the stage log directory <definitionDir>/"%U"/<pipeline>/<process> or
 * <definitionDir>/<user>/<pipeline>/<process>.
 */
public class LsfDefinitionFilePathResolver extends LsfFilePathResolver {

  private static final String FILE_SUFFIX = ".job";

  public LsfDefinitionFilePathResolver(StageExecutorRequest request, LsfExecutorParameters params) {
    super(
        request,
        params,
        (params == null || params.getDefinitionDir() == null) ? "" : params.getDefinitionDir(),
        FILE_SUFFIX);
  }
}
