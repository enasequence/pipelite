package pipelite.resolver;

import lombok.extern.flogger.Flogger;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;
import pipelite.task.TaskExecutionResultSerializer;
import pipelite.task.TaskExecutionResultType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Flogger
public class ExitCodeResolver implements ResultResolver<Integer> {

  private final Map<Integer, TaskExecutionResult> map;
  private final List<TaskExecutionResult> list;
  private final TaskExecutionResultExitCodeSerializer<Integer> serializer;

  public ExitCodeResolver(ExitCodeResolver resolver) {
    this.map = resolver.map;
    this.list = resolver.list;
    this.serializer = resolver.serializer;
  }

  public ExitCodeResolver(Map<Integer, TaskExecutionResult> map, List<TaskExecutionResult> list) {
    this.map = map;
    this.list = list;
    this.serializer = new TaskExecutionResultExitCodeSerializer(this);
  }

  @Override
  public TaskExecutionResult resolve(Integer cause) {
    if (cause == null) {
      log.atSevere().log(
          "Returning default permanent error. No task execution result for null exit code");
      return TaskExecutionResult.permanentError();
    }
    for (Map.Entry<Integer, TaskExecutionResult> entry : map.entrySet()) {
      if (entry.getKey().equals(cause)) {
        return entry.getValue();
      }
    }
    log.atSevere().log(
        "Returning default permanent error. No task execution result for exit code: %s",
        cause.toString());
    return TaskExecutionResult.permanentError();
  }

  @Override
  public List<TaskExecutionResult> results() {
    return list;
  }

  @Override
  public TaskExecutionResultSerializer<Integer> serializer() {
    return serializer;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<Integer, TaskExecutionResult> map = new HashMap<>();
    private final List<TaskExecutionResult> list = new ArrayList<>();

    public Builder() {
    }

    public Builder success(String result, int cause) {
      addResult(cause, new TaskExecutionResult(result, TaskExecutionResultType.SUCCESS));
      return this;
    }

    public Builder transientError(String result, int cause) {
      addResult(cause, new TaskExecutionResult(result, TaskExecutionResultType.TRANSIENT_ERROR));
      return this;
    }

    public Builder permanentError(String result, int cause) {
      addResult(cause, new TaskExecutionResult(result, TaskExecutionResultType.PERMANENT_ERROR));
      return this;
    }

    private void addResult(int cause, TaskExecutionResult taskExecutionResult) {
      this.map.put(cause, taskExecutionResult);
      this.list.add(taskExecutionResult);
    }

    public ExitCodeResolver build() {
      return new ExitCodeResolver(map, list);
    }
  }
}
