package pipelite.task;

import pipelite.instance.TaskInstance;
import pipelite.task.result.TaskExecutionResult;

/** Executable action */
public interface Task {
    TaskExecutionResult execute(TaskInstance instance);
}
