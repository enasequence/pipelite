package pipelite.resolver;

import pipelite.task.result.TaskExecutionResult;
import pipelite.task.result.serializer.TaskExecutionResultExitCodeSerializer;

import java.util.List;

public interface TaskExecutionResultResolver<T> {

    TaskExecutionResult success();

    TaskExecutionResult internalError();

    TaskExecutionResult resolveError(T cause);

    List<TaskExecutionResult> results();

    TaskExecutionResultExitCodeSerializer<T> exitCodeSerializer();
}
