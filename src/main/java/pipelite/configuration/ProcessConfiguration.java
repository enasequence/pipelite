package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.executor.TaskExecutorFactory;
import pipelite.stage.Stage;
import pipelite.stage.StageFactory;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.process", ignoreInvalidFields = true)
public class ProcessConfiguration {

  public ProcessConfiguration() {}

  /** Name of the process begin executed. */
  private String processName;

  private String stagesEnumName;

  private String stagesFactoryName;

  private String executorFactoryName;
}
