package pipelite.resolver;

import lombok.extern.flogger.Flogger;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;
import pipelite.task.TaskExecutionResultSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Flogger
public class ExitCodeResolver implements TaskExecutionResultResolver<Integer> {

  private final Map<Integer, TaskExecutionResult> map;
  private final List<TaskExecutionResult> list;
  private final TaskExecutionResultExitCodeSerializer<Integer> serializer;

  public ExitCodeResolver(Map<Integer, TaskExecutionResult> map, List<TaskExecutionResult> list) {
    this.map = map;
    this.list = list;
    this.serializer = new TaskExecutionResultExitCodeSerializer(this);
  }

  @Override
  public TaskExecutionResult resolve(Integer cause) {
    if (cause == null) {
      return TaskExecutionResult.success();
    }
    for (Map.Entry<Integer, TaskExecutionResult> entry : map.entrySet()) {
      if (entry.getKey().equals(cause)) {
        return entry.getValue();
      }
    }
    log.atSevere().log(
        "Could not resolve task execution result for for cause: " + cause.toString());
    return TaskExecutionResult.internalError();
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
      list.add(TaskExecutionResult.success());
    }

    public Builder success(Integer cause) {
      TaskExecutionResult result = TaskExecutionResult.success();
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public Builder transientError(Integer cause, String resultName) {
      TaskExecutionResult result = TaskExecutionResult.transientError(resultName);
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public Builder permanentError(Integer cause, String resultName) {
      TaskExecutionResult result = TaskExecutionResult.permanentError(resultName);
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public ExitCodeResolver build() {
      list.add(TaskExecutionResult.internalError());
      return new ExitCodeResolver(map, list);
    }
  }
}
