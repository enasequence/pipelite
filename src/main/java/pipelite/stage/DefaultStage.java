package pipelite.stage;

import lombok.AllArgsConstructor;
import lombok.Value;
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.executor.TaskExecutor;

@Value
@AllArgsConstructor
public class DefaultStage<T extends TaskExecutor> implements Stage {
  private final String stageName;
  private final Stage dependsOn;
  private final TaskConfigurationEx taskConfiguration;

  public DefaultStage(String stageName) {
    this.stageName = stageName;
    this.dependsOn = null;
    this.taskConfiguration = new TaskConfigurationEx(new TaskConfiguration());
  }

  public DefaultStage(String stageName, Stage dependsOn) {
    this.stageName = stageName;
    this.dependsOn = dependsOn;
    this.taskConfiguration = new TaskConfigurationEx(new TaskConfiguration());
  }
}
