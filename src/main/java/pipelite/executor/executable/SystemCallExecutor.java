package pipelite.executor.executable;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;

import java.util.List;

@Value
@Builder
@EqualsAndHashCode(callSuper=true)
public class SystemCallExecutor extends AbstractSystemCallExecutor {

  @NonNull private final String executable;
  private final Arguments arguments;

  public interface Arguments {
    List<String> getArguments(TaskInstance taskInstance);
  }

  public List<String> getArguments(TaskInstance taskInstance) {
    return arguments.getArguments(taskInstance);
  }

  @Override
  public TaskExecutionResult resolve(TaskInstance taskInstance, int exitCode) {
    return taskInstance.getResolver().resolve(exitCode);
  }
}
