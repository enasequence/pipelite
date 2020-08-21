package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.executor.TaskExecutor;
import pipelite.executor.TaskExecutorFactory;
import pipelite.resolver.ExceptionResolver;
import pipelite.stage.Stage;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.process")
public class ProcessConfiguration {

  public ProcessConfiguration() {}

  /** Name of the process begin executed. */
  private String processName;

  /** Name of the factory class that creates task executors. */
  private String executor;

  /** Name of the resolver class for task execution results. */
  private String resolver;

  /** Stages defined as an enumeration. */
  private String stages;

  public TaskExecutorFactory createExecutorFactory() {
    try {
      return ((TaskExecutorFactory) Class.forName(executor).newInstance());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public ExceptionResolver createResolver() {
    try {
      return (ExceptionResolver) Class.forName(resolver).newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public Stage[] getStageArray() {
    return (Stage[]) loadEnum(stages).getEnumConstants();
  }

  private Class<? extends Enum<?>> loadEnum(String name) {
    try {
      return ((Class<? extends Enum<?>>) Class.forName(name));
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }

  public Stage getStage(String name) {
    Stage[] stages = getStageArray();
    for (Stage stage : stages) {
      if (name.equals(stage.toString())) {
        return stage;
      }
    }
    throw new RuntimeException("Unknown stage:" + name);
  }
}
