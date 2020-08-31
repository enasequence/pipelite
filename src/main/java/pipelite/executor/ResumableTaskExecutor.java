package pipelite.executor;

import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

public interface ResumableTaskExecutor extends SerializableTaskExecutor {

  TaskExecutionResult resume(TaskInstance taskinstance);
}
