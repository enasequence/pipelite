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

import pipelite.task.executor.AbstractTaskExecutor;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import pipelite.task.state.TaskExecutionState;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.StageTask;

public class InternalStageExecutor extends AbstractTaskExecutor {
  private ExecutionInfo info;
  private StageTask task = null;

  public InternalStageExecutor(TaskExecutionResultExceptionResolver resolver) {
    super("", resolver);
  }

  @Override
  public void reset(StageInstance instance) {
    instance.setExecutionInstance(new ExecutionInstance());
  }

  public void execute(StageInstance instance) {
    Throwable exception = null;

    if (TaskExecutionState.ACTIVE_TASK == can_execute(instance)) {
      try {
        if (null != DefaultConfiguration.currentSet().getPropertyPrefixName()) {
          System.setProperty(
              DefaultConfiguration.currentSet().getPropertyPrefixName(),
              DefaultConfiguration.currentSet().getPropertySourceName());
        }

        Class<? extends StageTask> klass =
            DefaultConfiguration.currentSet().getStage(instance.getStageName()).getTaskClass();
        task = klass.getConstructor((Class[]) null).newInstance((Object[]) null);
        task.init(instance.getProcessID());
        task.execute();

      } catch (Throwable e) {
        e.printStackTrace();
        exception = e;
      } finally {
        info = new ExecutionInfo();
        info.setThrowable(exception);
        info.setExitCode(Integer.valueOf(resolver.exitCodeSerializer().serialize(resolver.resolveError(exception))));

        if (null != task) task.unwind();
      }
    }
  }

  @Override
  public ExecutionInfo get_info() {
    return info;
  }

  @Override
  public Class<? extends ExecutorConfig> getConfigClass() {
    return null;
  }

  @Override
  public void configure(ExecutorConfig rc) {
    // empty
  }
}
