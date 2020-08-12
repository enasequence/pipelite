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
package pipelite.task.executor;

import java.util.List;
import org.apache.log4j.Logger;
import pipelite.task.result.TaskExecutionResultType;
import pipelite.task.state.TaskExecutionState;
import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInstance;
import pipelite.task.result.TaskExecutionResultTranslator;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;

public abstract class AbstractTaskExecutor implements TaskExecutor {
  protected Logger log = Logger.getLogger(this.getClass());
  protected final String PIPELINE_NAME;
  protected final TaskExecutionResultTranslator TRANSLATOR;

  public AbstractTaskExecutor(String pipeline_name, TaskExecutionResultTranslator translator) {
    this.PIPELINE_NAME = pipeline_name;
    this.TRANSLATOR = translator;
  }

  @Override
  public TaskExecutionState can_execute(StageInstance instance) {
    // disabled stage
    if (!instance.isEnabled()) return TaskExecutionState.DISABLED_TASK;

    ExecutionInstance ei = instance.getExecutionInstance();

    // check permanent errors
    if (null != ei && null != ei.getFinish() && null != ei.getResultType()) {
      switch (ei.getResultType()) {
        case PERMANENT_ERROR:
          return TaskExecutionState.COMPLETED_TASK;
        default:
          return ei.getResultType() == TaskExecutionResultType.TRANSIENT_ERROR
              ? TaskExecutionState.ACTIVE_TASK
              : TaskExecutionState.DISABLED_TASK;
      }
    }

    return TaskExecutionState.ACTIVE_TASK;
  }

  protected void appendProperties(List<String> p_args, String... properties_to_pass) {
    for (String name : properties_to_pass) {
      for (Object p_name : System.getProperties().keySet())
        if (String.valueOf(p_name).startsWith(name))
          p_args.add(String.format("-D%s=%s", p_name, System.getProperties().get(p_name)));
    }
  }
}
