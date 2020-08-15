package pipelite.task.result.resolver;

import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipelite.task.result.ExecutionResult;

import java.util.*;

@Value
public class ExecutionResultExceptionResolver extends ExecutionResultResolver<Throwable> {

  private static final Logger logger =
      LoggerFactory.getLogger(ExecutionResultExceptionResolver.class);

  private static final ExecutionResult success = ExecutionResult.success();
  private static final ExecutionResult internalError = ExecutionResult.internalError();

  private final Map<Class<? extends Throwable>, ExecutionResult> map;
  private final List<ExecutionResult> list;

  @Override
  public ExecutionResult success() {
    return success;
  }

  @Override
  public ExecutionResult internalError() {
    return internalError;
  }

  @Override
  public ExecutionResult resolveError(Throwable cause) {
    for (Map.Entry<Class<? extends Throwable>, ExecutionResult> entry : map.entrySet()) {
      if (entry.getKey().isInstance(cause)) {
        return entry.getValue();
      }
    }
    logger.error("No execution result for cause: {}", cause.getClass().getCanonicalName());
    return internalError();
  }

  @Override
  public List<ExecutionResult> results() {
    return list;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<Class<? extends Throwable>, ExecutionResult> map = new HashMap<>();
    private final List<ExecutionResult> list = new ArrayList<>();

    public Builder() {
      list.add(success);
    }

    public Builder transientError(Class<? extends Throwable> cause, String resultName) {
      ExecutionResult result = ExecutionResult.transientError(resultName);
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public Builder permanentError(Class<? extends Throwable> cause, String resultName) {
      ExecutionResult result = ExecutionResult.permanentError(resultName);
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public ExecutionResultExceptionResolver build() {
      list.add(internalError);
      return new ExecutionResultExceptionResolver(map, list);
    }
  }
}
