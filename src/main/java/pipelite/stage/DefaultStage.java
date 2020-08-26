package pipelite.stage;

import lombok.AllArgsConstructor;
import lombok.Value;
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskFactory;

@Value
@AllArgsConstructor
public class DefaultStage<T extends TaskExecutor> implements Stage {
  private final String stageName;
  private final Stage dependsOn;
  private final TaskFactory taskFactory;
  private final TaskConfigurationEx taskConfiguration;

  public DefaultStage(String stageName, TaskFactory taskFactory) {
    this.stageName = stageName;
    this.dependsOn = null;
    this.taskFactory = taskFactory;
    this.taskConfiguration = new TaskConfigurationEx(new TaskConfiguration());
  }

  public DefaultStage(String stageName, TaskFactory taskFactory, Stage dependsOn) {
    this.stageName = stageName;
    this.dependsOn = dependsOn;
    this.taskFactory = taskFactory;
    this.taskConfiguration = new TaskConfigurationEx(new TaskConfiguration());
  }
}
