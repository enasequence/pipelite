/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor.executable.call;

import pipelite.executor.InternalTaskExecutor;
import pipelite.executor.executable.call.AbstractSystemCallExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskExecutionResult;

import java.util.List;

public class SystemCallInternalExecutor extends AbstractSystemCallExecutor {

  @Override
  public String getExecutable() {
    return InternalTaskExecutor.getInternalTaskExecutorExecutable();
  }

  @Override
  public List<String> getArguments(TaskInstance taskInstance) {
    return InternalTaskExecutor.getInternalTaskExecutorArgs(taskInstance);
  }

  @Override
  public TaskExecutionResult resolve(TaskInstance taskInstance, int exitCode) {
    return InternalTaskExecutor.getInternalTaskExecutionResult(taskInstance, exitCode);
  }
}
