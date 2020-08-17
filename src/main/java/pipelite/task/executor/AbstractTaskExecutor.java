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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import pipelite.task.result.TaskExecutionResultType;
import pipelite.resolver.ExceptionResolver;
import pipelite.task.state.TaskExecutionState;
import pipelite.task.instance.LatestTaskExecution;
import pipelite.task.instance.TaskInstance;

public abstract class AbstractTaskExecutor implements TaskExecutor {
  protected final Logger log = Logger.getLogger(this.getClass());
  protected final String PIPELINE_NAME;
  protected final ExceptionResolver resolver;

  public AbstractTaskExecutor(String pipeline_name, ExceptionResolver resolver) {
    this.PIPELINE_NAME = pipeline_name;
    this.resolver = resolver;
  }

  protected final String[] mergeJavaSystemProperties(String[] pp1, String[] pp2) {
    if (pp1 == null) {
      pp1 = new String[0];
    }
    if (pp2 == null) {
      pp2 = new String[0];
    }
    Set<String> set1 = new HashSet<>(Arrays.asList(pp1));
    Set<String> set2 = new HashSet<>(Arrays.asList(pp2));
    set1.addAll(set2);
    return set1.toArray(new String[set1.size()]);
  }

  protected final void addJavaSystemProperties(List<String> p_args, String... properties) {
    for (String property : properties) {
      String value = System.getProperty(property);
      if (value != null) {
        p_args.add(String.format("-D%s=%s", property, value));
      }
    }
  }

  public TaskExecutionState getTaskExecutionState(TaskInstance instance) {
    if (!instance.isEnabled()) {
      return TaskExecutionState.DISABLED;
    }

    LatestTaskExecution ei = instance.getLatestTaskExecution();

    // check permanent errors
    if (null != ei && null != ei.getEndTime() && null != ei.getResultType()) {
      switch (ei.getResultType()) {
        case PERMANENT_ERROR:
          return TaskExecutionState.COMPLETED;
        default:
          return ei.getResultType() == TaskExecutionResultType.TRANSIENT_ERROR
              ? TaskExecutionState.ACTIVE
              : TaskExecutionState.DISABLED;
      }
    }

    return TaskExecutionState.ACTIVE;
  }
}
