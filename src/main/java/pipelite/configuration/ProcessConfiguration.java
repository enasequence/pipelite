package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
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

  /** Name of the class that resolves results. */
  private String resolver;

  /** Stages defined as an enumeration. */
  private String stages;

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
