package pipelite.stage;

import lombok.AllArgsConstructor;
import lombok.Value;
import pipelite.configuration.TaskConfiguration;
import pipelite.task.Task;

@Value
@AllArgsConstructor
public class DefaultStage implements Stage {
  private final String stageName;
  private final Class<? extends Task> taskClass;
  private final Stage dependsOn;
  private final TaskConfiguration taskConfiguration;

  public DefaultStage(String stageName, Class<? extends Task> taskClass) {
    this.stageName = stageName;
    this.taskClass = taskClass;
    this.dependsOn = null;
    this.taskConfiguration = new TaskConfiguration();
  }
}
