package pipelite.task;

import lombok.Value;

@Value
public class TaskInfo {
  private final String processName;
  private final String processId;
  private final String taskName;
}
