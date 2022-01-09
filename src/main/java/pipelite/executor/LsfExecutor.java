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
import pipelite.executor.cmd.CmdRunnerUtils;
import pipelite.executor.task.RetryTask;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.ExecutorParameters;
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

  private static final String JOB_FILE_SUFFIX = ".job";

  @Override
  public final String getSubmitCmd(StageExecutorRequest request) {

    StringBuilder cmd = new StringBuilder();
    cmd.append(BSUB_CMD);

    // Write both stderr and stdout to the stdout file. The parameters that are specified in the
    // command line override other parameters.

    addArgument(cmd, "-oo");
    addArgument(cmd, getOutFile());

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
  protected void prepareAsyncSubmit(StageExecutorRequest request) {
    super.prepareAsyncSubmit(request);
    definitionFile = getDefinitionFile(request, getExecutorParams());
    URL definitionUrl =
        ExecutorParameters.validateUrl(getExecutorParams().getDefinition(), "definition");
    final String definition =
        applyDefinitionParameters(
            CmdRunnerUtils.read(definitionUrl), getExecutorParams().getParameters());
    RetryTask.DEFAULT.execute(
        r -> {
          getCmdRunner().writeFile(definition, Paths.get(definitionFile));
          return null;
        });
  }

  public void setDefinitionFile(String definitionFile) {
    this.definitionFile = definitionFile;
  }

  public static <T extends LsfExecutorParameters> String getDefinitionFile(
      StageExecutorRequest context, T params) {
    return getWorkDir(context, params)
        .resolve(context.getStage().getStageName() + JOB_FILE_SUFFIX)
        .toString();
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
