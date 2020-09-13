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
package pipelite.executor;

import java.time.Duration;

import pipelite.task.ConfigurableTaskParameters;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

public interface PollableExecutor extends SerializableExecutor {

  TaskExecutionResult poll(TaskInstance taskInstance);

  default Duration getPollFrequency(TaskInstance taskInstance) {
    if (taskInstance.getTaskParameters().getPollDelay() != null) {
      return taskInstance.getTaskParameters().getPollDelay();
    }
    return ConfigurableTaskParameters.DEFAULT_POLL_DELAY;
  }
}
