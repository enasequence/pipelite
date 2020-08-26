package pipelite.task;

import pipelite.instance.TaskInstance;

/** Executable action */
public interface Task {
    void execute(TaskInstance instance);
}
