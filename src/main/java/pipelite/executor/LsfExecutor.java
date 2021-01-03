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

import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.stage.*;
import pipelite.stage.parameters.LsfExecutorParameters;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
public class LsfExecutor extends AbstractLsfExecutor<LsfExecutorParameters>
    implements JsonSerializableExecutor {

  private String definitionFile;

  @Override
  public final String getDispatcherCmd(String pipelineName, String processId, Stage stage) {

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
    return cmd.toString();
  }

  @Override
  protected void beforeSubmit(String pipelineName, String processId, Stage stage) {
    // TODO: copy definition file into the working directory unless
    // getExecutorParams().getDefinition() already points there
    definitionFile = "TODO";
  }
}
