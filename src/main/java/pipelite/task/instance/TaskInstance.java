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
package pipelite.task.instance;

import lombok.Data;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;

@Data
public class TaskInstance {
  private int executionCount;
  private String processId;
  private String taskName;
  private String processName;
  private String dependsOn;
  private boolean enabled;
  private int memory;
  private int cores;
  private String[] propertiesPass;
  private ExecutorConfig[] taskExecutorConfig;
  private LatestTaskExecution latestTaskExecution = new LatestTaskExecution();

  public TaskInstance() {}

  public TaskInstance(TaskInstance other) {
    this.executionCount = other.executionCount;
    this.processId = other.processId;
    this.taskName = other.taskName;
    this.processName = other.processName;
    this.dependsOn = other.dependsOn;
    this.enabled = other.enabled;
    this.memory = other.memory;
    this.cores = other.cores;
    this.propertiesPass = other.propertiesPass;
    this.taskExecutorConfig = other.taskExecutorConfig;
    this.latestTaskExecution = other.latestTaskExecution;
  }

  public <T extends ExecutorConfig> T getTaskExecutorConfig(Class<? extends ExecutorConfig> klass) {
    if (null == taskExecutorConfig) {
      return null;
    }
    for (ExecutorConfig config : taskExecutorConfig) {
      try {
        return (T) klass.cast(config);
      } catch (ClassCastException cce) {
      }
    }
    return null;
  }

  public void setTaskExecutorConfig(ExecutorConfig... taskExecutorConfig) {
    this.taskExecutorConfig = taskExecutorConfig;
  }
}
