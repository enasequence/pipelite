package pipelite.stage;

import lombok.AllArgsConstructor;
import lombok.Value;
import pipelite.configuration.TaskConfiguration;
import pipelite.task.Task;
import pipelite.task.TaskFactory;

@Value
@AllArgsConstructor
public class DefaultStage<T extends Task> implements Stage<T> {
  private final String stageName;
  private final TaskFactory<T> taskFactory;
  private final Stage dependsOn;
  private final TaskConfiguration taskConfiguration;

  public DefaultStage(String stageName, TaskFactory<T> taskFactory) {
    this.stageName = stageName;
    this.taskFactory = taskFactory;
    this.dependsOn = null;
    this.taskConfiguration = new TaskConfiguration();
  }

  public DefaultStage(String stageName, TaskFactory<T> taskFactory, Stage dependsOn) {
    this.stageName = stageName;
    this.taskFactory = taskFactory;
    this.dependsOn = dependsOn;
    this.taskConfiguration = new TaskConfiguration();
  }
}
