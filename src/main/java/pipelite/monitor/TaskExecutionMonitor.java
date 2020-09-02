package pipelite.monitor;

import pipelite.task.TaskExecutionResult;

public interface TaskExecutionMonitor {

  MonitoredTaskExecutionResult monitor();

  void done(TaskExecutionResult taskExecutionResult);


}
