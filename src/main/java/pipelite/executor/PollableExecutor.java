package pipelite.executor;

import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

import java.time.Duration;

public interface PollableExecutor extends SerializableExecutor {

  TaskExecutionResult poll(TaskInstance taskinstance);

  Duration DEFAULT_POLL_DELAY = Duration.ofMinutes(1);

  default Duration getPollFrequency(TaskInstance taskInstance) {
    if (taskInstance.getTaskParameters().getPollDelay() != null) {
      return taskInstance.getTaskParameters().getPollDelay();
    }
    return DEFAULT_POLL_DELAY;
  }
}
