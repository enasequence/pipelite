package pipelite.task.result.serializer;

import lombok.Value;
import pipelite.task.result.ExecutionResult;
import pipelite.task.result.resolver.ExecutionResultResolver;

import java.util.List;

@Value
public class ExecutionResultExitCodeSerializer implements ExecutionResultSerializer<Integer> {

  private final ExecutionResultResolver resolver;

  @Override
  public Integer serialize(ExecutionResult result) {
    int value = resolver.results().indexOf(result);
    checkValue(value);
    return value;
  }

  @Override
  public ExecutionResult deserialize(Integer value) {
    checkValue(value);
    List<ExecutionResult> results = resolver.results();
    return results.get(value);
  }

  private static void checkValue(Integer value) {
    if (value < 0 || value > 255) {
      throw new IllegalArgumentException("Failed to serialize execution result");
    }
  }
}
