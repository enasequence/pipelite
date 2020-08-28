package pipelite.resolver;

import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultSerializer;

import java.util.List;

public class DefaultExceptionResolver extends ExceptionResolver {

  public DefaultExceptionResolver() {
    super(ExceptionResolver.builder().build());
  }
}
