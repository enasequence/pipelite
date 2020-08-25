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
import pipelite.configuration.TaskConfiguration;
import pipelite.executor.AbstractTaskExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskInstance;

public class InternalTaskExecutor extends AbstractTaskExecutor {

  public InternalTaskExecutor(
      ProcessConfiguration processConfiguration, TaskConfiguration taskConfiguration) {
    super(processConfiguration, taskConfiguration);
  }

  public ExecutionInfo execute(TaskInstance taskInstance) {
    Throwable exception = null;

    try {
      String stageName = taskInstance.getPipeliteStage().getStageName();
      TaskExecutor taskExecutor =
          processConfiguration
              .getStage(stageName)
              .getTaskExecutorFactory()
              .createTaskExecutor(processConfiguration, taskConfiguration);
      taskExecutor.execute(taskInstance);

    } catch (Throwable e) {
      e.printStackTrace();
      exception = e;
    } finally {
      ExecutionInfo info = new ExecutionInfo();
      info.setThrowable(exception);
      info.setExitCode(resolver.exitCodeSerializer().serialize(resolver.resolveError(exception)));
      return info;
    }
  }
}
