package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.executor.TaskExecutorFactory;
import pipelite.resolver.ExceptionResolver;
import pipelite.stage.Stage;
import pipelite.stage.StageFactory;

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

  /** Name of the stages declaration. */
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
    Class cls;
    try {
      cls = Class.forName(stages);
      if (StageFactory.class.isAssignableFrom(cls)) {
        StageFactory stageFactory = ((StageFactory) cls.newInstance());
        return stageFactory.create();
      } else {
        return (Stage[]) loadEnum(stages).getEnumConstants();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private Class<? extends Enum<?>> loadEnum(String name) throws ClassNotFoundException {
    return ((Class<? extends Enum<?>>) Class.forName(name));
  }

  public Stage getStage(String name) {
    Stage[] stages = getStageArray();
    for (Stage stage : stages) {
      if (name.equals(stage.getStageName())) {
        return stage;
      }
    }
    throw new RuntimeException("Unknown stage:" + name);
  }
}
