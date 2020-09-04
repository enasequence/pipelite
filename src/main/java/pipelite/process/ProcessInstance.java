package pipelite.process;

import com.google.common.flogger.FluentLogger;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.log.LogKey;
import pipelite.task.TaskInstance;

import java.util.HashSet;
import java.util.List;

@Flogger
@Value
@Builder
public class ProcessInstance {
  private final String processName;
  private final String processId;
  @EqualsAndHashCode.Exclude private final Integer priority;
  @EqualsAndHashCode.Exclude private final List<TaskInstance> tasks;

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

      for (TaskInstance taskInstance : tasks) {
        if (!taskInstance.validate()) {
          isSuccess = false;
        }
        if (taskInstance.getProcessName() != null) {
          if (!taskInstance.getProcessName().equals(processName)) {
            logContext(log.atSevere())
                .log("Conflicting process name in task %s", taskInstance.getProcessName());
            isSuccess = false;
          }
        }
        if (taskInstance.getTaskName() != null) {
          if (taskNames.contains(taskInstance.getTaskName())) {
            taskInstance
                .logContext(log.atSevere())
                .log("Duplicate task name: %s", taskInstance.getTaskName());
            isSuccess = false;
          }
          taskNames.add(taskInstance.getTaskName());
        }
      }
    }
    return isSuccess;
  }

  public FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PROCESS_NAME, processName).with(LogKey.PROCESS_ID, processId);
  }
}
