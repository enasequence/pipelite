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
package pipelite.executor;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.executor.cmd.CmdRunnerUtils;
import pipelite.retryable.RetryableExternalAction;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.ExecutorParametersValidator;
import pipelite.stage.parameters.LsfExecutorParameters;
import pipelite.stage.path.LsfDefinitionFilePathResolver;
import pipelite.stage.path.LsfFilePathResolver;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
public class LsfExecutor extends AbstractLsfExecutor<LsfExecutorParameters>
    implements JsonSerializableExecutor {

  // Json deserialization requires a no argument constructor.
  public LsfExecutor() {}

  private String definitionFile;

  @Override
  public final String getSubmitCmd(StageExecutorRequest request) {

    StringBuilder cmd = getSharedSubmitCmd(request);

    switch (getExecutorParams().getFormat()) {
      case JSDL:
        addArgument(cmd, "-jsdl");
        break;
      case YAML:
        addArgument(cmd, "-yaml");
        break;
      case JSON:
        addArgument(cmd, "-json");
        break;
    }
    addArgument(cmd, definitionFile);
    return cmd.toString() + " " + getCmd();
  }

  @Override
  protected void prepareSubmit() {
    StageExecutorRequest request = getRequest();
    LsfDefinitionFilePathResolver resolver =
        new LsfDefinitionFilePathResolver(request, getExecutorParams());
    LsfFilePathResolver.Format format = LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN;
    if (!getCmdRunner().createDir(Paths.get(resolver.getDir(format)))) {
      throw new PipeliteException("Failed to create LSF definition dir");
    }
    definitionFile = resolver.getFile(format);
    URL definitionUrl =
        ExecutorParametersValidator.validateUrl(getExecutorParams().getDefinition(), "definition");
    final String definition =
        applyDefinitionParameters(
            CmdRunnerUtils.read(definitionUrl), getExecutorParams().getParameters());
    RetryableExternalAction.execute(
        () -> {
          getCmdRunner().writeFile(definition, Paths.get(definitionFile));
          return null;
        });
  }

  public void setDefinitionFile(String definitionFile) {
    this.definitionFile = definitionFile;
  }

  public String applyDefinitionParameters(String definition, Map<String, String> params) {
    if (params == null) {
      return definition;
    }
    AtomicReference<String> str = new AtomicReference<>(definition);
    params.forEach((key, value) -> str.set(str.get().replace(key, value)));
    return str.get();
  }
}
