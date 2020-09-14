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

import pipelite.executor.InternalExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.LocalRunner;
import pipelite.task.Task;

public final class LsfLocalTaskExecutor extends LsfExecutor {

  /** The actual TaskExecutor to be used. */
  private final TaskExecutor taskExecutor;

  public LsfLocalTaskExecutor(TaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  @Override
  public CommandRunner getCmdRunner() {
    return new LocalRunner();
  }

  @Override
  public String getCmd(Task task) {
    return InternalExecutor.getCmd(task, taskExecutor);
  }
}
