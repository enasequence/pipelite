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
package pipelite.executor.lsf;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.executor.InternalExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.SshRunner;
import pipelite.task.TaskInstance;

@Flogger
public final class LsfSshTaskExecutor extends LsfExecutor {

  /** The actual TaskExecutor to be used. */
  private final TaskExecutor taskExecutor;

  public LsfSshTaskExecutor(TaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  @Override
  public CommandRunner getCmdRunner() {
    return new SshRunner();
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return InternalExecutor.getCmd(taskInstance, taskExecutor);
  }
}
