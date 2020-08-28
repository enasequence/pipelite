package pipelite.resolver;

import lombok.extern.flogger.Flogger;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;
import pipelite.task.TaskExecutionResultSerializer;
import pipelite.task.TaskExecutionResultType;

import java.util.*;

@Flogger
public class ExceptionResolver implements ResultResolver<Throwable> {

  private final Map<Class<? extends Throwable>, TaskExecutionResult> map;
  private final List<TaskExecutionResult> list;
  private final TaskExecutionResultExitCodeSerializer<Throwable> serializer;

  public ExceptionResolver(ExceptionResolver resolver) {
    this.map = resolver.map;
    this.list = resolver.list;
    this.serializer = resolver.serializer;
  }

  public ExceptionResolver(
      Map<Class<? extends Throwable>, TaskExecutionResult> map, List<TaskExecutionResult> list) {
    this.map = map;
    this.list = list;
    this.serializer = new TaskExecutionResultExitCodeSerializer(this);
  }

  @Override
  public TaskExecutionResult resolve(Throwable cause) {
    if (cause == null) {
      log.atSevere().log(
          "Returning default permanent error. No task execution result for null exception");
      return TaskExecutionResult.defaultPermanentError();
    }
    for (Map.Entry<Class<? extends Throwable>, TaskExecutionResult> entry : map.entrySet()) {
      if (entry.getKey().isInstance(cause)) {
        return entry.getValue();
      }
    }
    log.atSevere().log(
        "Returning default permanent error. No task execution result for exception: %s",
        cause.toString());
    return TaskExecutionResult.defaultPermanentError();
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

    public Builder() {}

    public Builder success(String result, Class<? extends Throwable> cause) {
      TaskExecutionResult taskExecutionResult =
          new TaskExecutionResult(result, TaskExecutionResultType.SUCCESS);
      addResult(cause, taskExecutionResult);
      return this;
    }

    public Builder transientError(String result, Class<? extends Throwable> cause) {
      TaskExecutionResult taskExecutionResult =
          new TaskExecutionResult(result, TaskExecutionResultType.TRANSIENT_ERROR);
      addResult(cause, taskExecutionResult);
      return this;
    }

    public Builder permanentError(String result, Class<? extends Throwable> cause) {
      TaskExecutionResult taskExecutionResult =
          new TaskExecutionResult(result, TaskExecutionResultType.PERMANENT_ERROR);
      addResult(cause, taskExecutionResult);
      return this;
    }

    private void addResult(
        Class<? extends Throwable> cause, TaskExecutionResult taskExecutionResult) {
      this.map.put(cause, taskExecutionResult);
      this.list.add(taskExecutionResult);
    }

    public ExceptionResolver build() {
      return new ExceptionResolver(map, list);
    }
  }
}
