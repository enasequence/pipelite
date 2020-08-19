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
package uk.ac.ebi.ena.sra.pipeline.launcher;

import pipelite.configuration.ProcessConfiguration;
import pipelite.task.executor.AbstractTaskExecutor;
import pipelite.task.instance.TaskInstance;
import pipelite.task.state.TaskExecutionState;
import pipelite.task.Task;

public class InternalStageExecutor extends AbstractTaskExecutor {

  private final ProcessConfiguration processConfiguration;

  private ExecutionInfo info;

  public InternalStageExecutor(ProcessConfiguration processConfiguration) {
    super("", processConfiguration.createResolver());
    this.processConfiguration = processConfiguration;
  }

  @Override
  public void reset(TaskInstance instance) {
    instance.getPipeliteStage().resetExecution();
  }

  public void execute(TaskInstance instance) {
    Throwable exception = null;

    if (TaskExecutionState.ACTIVE == getTaskExecutionState(instance)) {
      try {

        Class<? extends Task> taskClass =
            processConfiguration
                .getStage(instance.getPipeliteStage().getStageName())
                .getTaskClass();
        Task task = taskClass.newInstance();
        task.run();

      } catch (Throwable e) {
        e.printStackTrace();
        exception = e;
      } finally {
        info = new ExecutionInfo();
        info.setThrowable(exception);
        info.setExitCode(
                resolver.exitCodeSerializer().serialize(resolver.resolveError(exception)));
      }
    }
  }

  @Override
  public ExecutionInfo get_info() {
    return info;
  }
}
