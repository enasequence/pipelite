package pipelite.executor;

import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;

public interface TaskExecutorFactory {
  TaskExecutor create(ProcessConfiguration processConfiguration, TaskConfiguration taskConfiguration);
}
