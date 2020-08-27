package pipelite.resolver;

import lombok.extern.flogger.Flogger;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;
import pipelite.task.TaskExecutionResultSerializer;

import java.util.*;

@Flogger
public class ExceptionResolver implements TaskExecutionResultResolver<Throwable> {

  private final Map<Class<? extends Throwable>, TaskExecutionResult> map;
  private final List<TaskExecutionResult> list;
  private final TaskExecutionResultExitCodeSerializer<Throwable> serializer;

  public ExceptionResolver(
      Map<Class<? extends Throwable>, TaskExecutionResult> map, List<TaskExecutionResult> list) {
    this.map = map;
    this.list = list;
    this.serializer = new TaskExecutionResultExitCodeSerializer(this);
  }

  @Override
  public TaskExecutionResult resolve(Throwable cause) {
    if (cause == null) {
      return TaskExecutionResult.success();
    }
    for (Map.Entry<Class<? extends Throwable>, TaskExecutionResult> entry : map.entrySet()) {
      if (entry.getKey().isInstance(cause)) {
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
  public TaskExecutionResultSerializer<Throwable> serializer() {
    return serializer;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<Class<? extends Throwable>, TaskExecutionResult> map = new HashMap<>();
    private final List<TaskExecutionResult> list = new ArrayList<>();

    public Builder() {
      list.add(TaskExecutionResult.success());
    }

    public Builder transientError(Class<? extends Throwable> cause, String resultName) {
      TaskExecutionResult result = TaskExecutionResult.transientError(resultName);
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public Builder permanentError(Class<? extends Throwable> cause, String resultName) {
      TaskExecutionResult result = TaskExecutionResult.permanentError(resultName);
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public ExceptionResolver build() {
      list.add(TaskExecutionResult.internalError());
      return new ExceptionResolver(map, list);
    }
  }
}
