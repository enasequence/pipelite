package pipelite.resolver;

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipelite.task.result.TaskExecutionResult;

import java.util.*;

@Value
public class ConcreteExceptionResolver extends AbstractResolver<Throwable>
    implements ExceptionResolver {

  private static final Logger logger = LoggerFactory.getLogger(ExceptionResolver.class);

  private static final TaskExecutionResult success = TaskExecutionResult.success();
  private static final TaskExecutionResult internalError = TaskExecutionResult.internalError();

  @EqualsAndHashCode.Exclude private final Map<Class<? extends Throwable>, TaskExecutionResult> map;
  @EqualsAndHashCode.Exclude private final List<TaskExecutionResult> list;

  @Override
  public TaskExecutionResult success() {
    return success;
  }

  @Override
  public TaskExecutionResult internalError() {
    return internalError;
  }

  @Override
  public TaskExecutionResult resolveError(Throwable cause) {
    if (cause == null) {
      return success();
    }
    for (Map.Entry<Class<? extends Throwable>, TaskExecutionResult> entry : map.entrySet()) {
      if (entry.getKey().isInstance(cause)) {
        return entry.getValue();
      }
    }
    logger.error("No execution result for cause: {}", cause.getClass().getCanonicalName());
    return internalError();
  }

  @Override
  public List<TaskExecutionResult> results() {
    return list;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<Class<? extends Throwable>, TaskExecutionResult> map = new HashMap<>();
    private final List<TaskExecutionResult> list = new ArrayList<>();

    public Builder() {
      list.add(success);
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

    public ConcreteExceptionResolver build() {
      list.add(internalError);
      return new ConcreteExceptionResolver(map, list);
    }
  }
}
