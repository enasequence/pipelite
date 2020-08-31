package pipelite.monitor;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import pipelite.task.TaskExecutionResult;

@Value
@Builder
public class MonitoredTaskExecutionResult {
  @NonNull private final String processName;
  @NonNull private final String processId;
  @NonNull private final String taskName;
  private final TaskExecutionResult taskExecutionResult;
}
