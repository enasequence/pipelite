package pipelite.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import pipelite.task.TaskExecutionResult;

public interface TaskExecutionMonitor {

  MonitoredTaskExecutionResult monitor();

  void done(TaskExecutionResult taskExecutionResult);


}
