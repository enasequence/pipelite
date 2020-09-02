package pipelite.executor;

import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

import java.time.Duration;

public interface PollableTaskExecutor extends SerializableTaskExecutor {

  TaskExecutionResult poll(TaskInstance taskinstance);

  public static Duration DEFAULT_POLL_FREQUENCY = Duration.ofMinutes(1);

  default Duration getPollFrequency(TaskInstance taskInstance) {
    if (taskInstance.getTaskParameters().getPollFrequency() != null) {
      return taskInstance.getTaskParameters().getPollFrequency();
    }
    return DEFAULT_POLL_FREQUENCY;
  }
}
