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
package pipelite.executor;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.executor.cmd.CmdRunnerUtils;
import pipelite.retryable.RetryableExternalAction;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.ExecutorParametersValidator;
import pipelite.stage.parameters.LsfExecutorParameters;

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
        addCmdArgument(cmd, "-jsdl");
        break;
      case YAML:
        addCmdArgument(cmd, "-yaml");
        break;
      case JSON:
        addCmdArgument(cmd, "-json");
        break;
    }
    addCmdArgument(cmd, definitionFile);
    return cmd + " " + getCmd();
  }

  @Override
  protected void prepareJob() {
    definitionFile =
        RetryableExternalAction.execute(
            () -> {
              // Create temp file.
              String tempFile = getCmdRunner().createTempFile();

              URL definitionUrl =
                  ExecutorParametersValidator.validateUrl(
                      getExecutorParams().getDefinition(), "definition");
              final String definition =
                  applyDefinitionParameters(
                      CmdRunnerUtils.read(definitionUrl), getExecutorParams().getParameters());

              getCmdRunner().writeFile(definition, Paths.get(tempFile));
              return tempFile;
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
