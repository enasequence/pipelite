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
package pipelite.task;

import com.google.common.flogger.FluentLogger;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.executor.TaskExecutor;
import pipelite.log.LogKey;

@Flogger
@Value
@Builder
public class TaskInstance {
  private final String processName;
  private final String processId;
  private final String taskName;
  @EqualsAndHashCode.Exclude private final TaskExecutor executor;
  @EqualsAndHashCode.Exclude private final TaskInstance dependsOn;
  @EqualsAndHashCode.Exclude private final TaskParameters taskParameters;

  public TaskInstance(
      String processName,
      String processId,
      String taskName,
      TaskExecutor executor,
      TaskInstance dependsOn,
      TaskParameters taskParameters) {
    this.processName = processName;
    this.processId = processId;
    this.taskName = taskName;
    this.executor = executor;
    this.dependsOn = dependsOn;
    if (taskParameters != null) {
      this.taskParameters = taskParameters;
    } else {
      this.taskParameters = TaskParameters.builder().build();
    }
  }

  public boolean validate() {
    boolean isSuccess = true;
    if (processName == null || processName.isEmpty()) {
      logContext(log.atSevere()).log("Process name is missing");
      isSuccess = false;
    }
    if (processId == null || processId.isEmpty()) {
      logContext(log.atSevere()).log("Process id is missing");
      isSuccess = false;
    }
    if (taskName == null || taskName.isEmpty()) {
      logContext(log.atSevere()).log("Task name is missing");
      isSuccess = false;
    }
    if (executor == null) {
      logContext(log.atSevere()).log("Executor is missing");
      isSuccess = false;
    }
    if (taskParameters == null) {
      logContext(log.atSevere()).log("Task parameters are missing");
      isSuccess = false;
    }
    return isSuccess;
  }

  public FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.TASK_NAME, taskName);
  }
}
