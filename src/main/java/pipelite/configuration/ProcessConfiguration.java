package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.executor.TaskExecutor;
import pipelite.instance.ProcessInstanceFactory;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.process", ignoreInvalidFields = true)
public class ProcessConfiguration {

  public ProcessConfiguration() {}

  /** Name of the process begin executed. */
  private String processName;

  /** Name of the process executor factory class. */
  private String processFactoryName;

  private ProcessInstanceFactory processFactory;

  private ProcessInstanceFactory getProcessFactory() {
    return processFactory;
  }

  public static ProcessInstanceFactory getProcessFactory(
      ProcessConfiguration processConfiguration) {
    if (processConfiguration.getProcessFactory() != null) {
      return processConfiguration.getProcessFactory();
    }
    if (processConfiguration.getProcessFactoryName() != null) {
      try {
        Class cls = Class.forName(processConfiguration.getProcessFactoryName());
        if (ProcessInstanceFactory.class.isAssignableFrom(cls)) {
          ProcessInstanceFactory factory = ((ProcessInstanceFactory) cls.newInstance());
          processConfiguration.setProcessFactory(factory);
          return factory;
        }
      } catch (Exception ex) {
        throw new RuntimeException("Could not create process factory", ex);
      }
    }
    throw new RuntimeException("Could not create process factory");
  }
}
