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
package pipelite.executor.command;

import pipelite.executor.CommandExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.SshRunner;
import pipelite.task.TaskInstance;

public final class SshCommandExecutor extends CommandExecutor {

  /** The actual command string to be executed. */
  private final String cmd;

  public SshCommandExecutor(String cmd) {
    this.cmd = cmd;
  }

  @Override
  public final CommandRunner getCmdRunner() {
    return new SshRunner();
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return cmd;
  }
}
