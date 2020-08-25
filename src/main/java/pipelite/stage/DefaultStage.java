package pipelite.stage;

import lombok.AllArgsConstructor;
import lombok.Value;
import pipelite.configuration.TaskConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.executor.TaskExecutorFactory;

@Value
@AllArgsConstructor
public class DefaultStage<T extends TaskExecutor> implements Stage<T> {
  private final String stageName;
  private final TaskExecutorFactory<T> taskExecutorFactory;
  private final Stage dependsOn;
  private final TaskConfiguration taskConfiguration;

  public DefaultStage(String stageName, TaskExecutorFactory<T> taskExecutorFactory) {
    this.stageName = stageName;
    this.taskExecutorFactory = taskExecutorFactory;
    this.dependsOn = null;
    this.taskConfiguration = new TaskConfiguration();
  }

  public DefaultStage(
      String stageName, TaskExecutorFactory<T> taskExecutorFactory, Stage dependsOn) {
    this.stageName = stageName;
    this.taskExecutorFactory = taskExecutorFactory;
    this.dependsOn = dependsOn;
    this.taskConfiguration = new TaskConfiguration();
  }
}
