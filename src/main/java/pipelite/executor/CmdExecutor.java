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

import static pipelite.stage.executor.StageExecutorResultAttribute.EXIT_CODE;

import com.google.common.primitives.Ints;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.executor.cmd.CmdRunner;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.CmdExecutorParameters;

/** Executes a command. Must be serializable to json. */
@Flogger
@Getter
@Setter
public class CmdExecutor<T extends CmdExecutorParameters> extends SyncExecutor<T>
    implements JsonSerializableExecutor {

  // Json deserialization requires a no argument constructor.
  public CmdExecutor() {}

  /** The command to be executed. */
  private String cmd;

  @Override
  public StageExecutorResult execute() {
    CmdRunner cmdRunner = CmdRunner.create(getExecutorParams());
    StageExecutorResult result = cmdRunner.execute(cmd);
    if (getExecutorParams()
        .getPermanentErrors()
        .contains(Ints.tryParse(result.attribute(EXIT_CODE)))) {
      result.state(StageExecutorState.PERMANENT_ERROR);
    }
    return result;
  }

  @Override
  public void terminate() {}

  @Override
  public String toString() {
    return serialize();
  }
}
