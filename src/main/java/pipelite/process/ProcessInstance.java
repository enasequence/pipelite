package pipelite.process;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import pipelite.task.TaskInstance;

import java.util.List;

@Value
@Builder
public class ProcessInstance {
  private final String processName;
  private final String processId;
  @EqualsAndHashCode.Exclude private final Integer priority;
  @EqualsAndHashCode.Exclude private final List<TaskInstance> tasks;
}
