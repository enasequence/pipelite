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
package pipelite.process;

import com.google.common.flogger.FluentLogger;
import java.util.HashSet;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.log.LogKey;
import pipelite.task.Task;

@Flogger
@Value
@Builder
public class Process {
  private final String processName;
  private final String processId;
  @EqualsAndHashCode.Exclude private final int priority;
  @EqualsAndHashCode.Exclude private final List<Task> tasks;

  public enum ValidateMode {
    WITH_TASKS,
    WITHOUT_TASKS
  };

  public boolean validate(ValidateMode validateMode) {
    boolean isSuccess = true;
    if (processName == null || processName.isEmpty()) {
      logContext(log.atSevere()).log("Process name is missing");
      isSuccess = false;
    }
    if (processId == null || processId.isEmpty()) {
      logContext(log.atSevere()).log("Process id is missing");
      isSuccess = false;
    }

    if (validateMode == ValidateMode.WITHOUT_TASKS) {
      return isSuccess;
    }

    if (tasks == null || tasks.isEmpty()) {
      logContext(log.atSevere()).log("No tasks");
      isSuccess = false;

    } else {
      HashSet<String> taskNames = new HashSet<>();

      for (Task task : tasks) {
        if (!task.validate()) {
          isSuccess = false;
        }
        if (task.getProcessName() != null) {
          if (!task.getProcessName().equals(processName)) {
            logContext(log.atSevere())
                .log("Conflicting process name in task %s", task.getProcessName());
            isSuccess = false;
          }
        }
        if (task.getTaskName() != null) {
          if (taskNames.contains(task.getTaskName())) {
            task.logContext(log.atSevere()).log("Duplicate task name: %s", task.getTaskName());
            isSuccess = false;
          }
          taskNames.add(task.getTaskName());
        }
      }
    }
    return isSuccess;
  }

  public FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PROCESS_NAME, processName).with(LogKey.PROCESS_ID, processId);
  }
}
