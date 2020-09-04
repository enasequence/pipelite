package pipelite.executor.runner;

import pipelite.task.TaskParameters;

public interface CommandRunner {
  CommandRunnerResult execute(String cmd, TaskParameters taskParameters);
}
