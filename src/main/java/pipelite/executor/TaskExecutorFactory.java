package pipelite.executor;

import pipelite.configuration.TaskConfigurationEx;

public interface TaskExecutorFactory {
  TaskExecutor createTaskExecutor(TaskConfigurationEx taskConfiguration);
}
