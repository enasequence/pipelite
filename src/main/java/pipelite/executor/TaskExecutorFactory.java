package pipelite.executor;

import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;

public interface TaskExecutorFactory<T extends TaskExecutor> {
  T createTaskExecutor(ProcessConfiguration processConfiguration, TaskConfiguration taskConfiguration);
}
