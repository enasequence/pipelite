package pipelite.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import pipelite.task.TaskExecutionResult;

public interface TaskExecutionMonitor {

  MonitoredTaskExecutionResult monitor();

  void done(TaskExecutionResult taskExecutionResult);

  static String serialize(TaskExecutionMonitor monitor) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(monitor);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  static TaskExecutionMonitor deserialize(String className, String data) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return (TaskExecutionMonitor) objectMapper.readValue(data, Class.forName(className));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
