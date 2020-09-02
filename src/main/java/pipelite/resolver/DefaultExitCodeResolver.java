package pipelite.resolver;

import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;

public class DefaultExitCodeResolver extends ExitCodeResolver {

  public DefaultExitCodeResolver() {
    super(
        ExitCodeResolver.builder()
            .success(
                TaskExecutionResult.success().getResult(),
                TaskExecutionResultExitCodeSerializer.EXIT_CODE_DEFAULT_SUCCESS)
            .build());
  }
}
