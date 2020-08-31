package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.executor.TaskExecutor;
import pipelite.instance.ProcessInstanceFactory;
import pipelite.instance.ProcessInstanceSource;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.process", ignoreInvalidFields = true)
public class ProcessConfiguration {

  public ProcessConfiguration() {}

  /** Name of the process begin executed. */
  private String processName;

  private String processFactoryName;

  private String processSourceName;

  private ProcessInstanceFactory processFactory;

  private ProcessInstanceFactory getProcessFactory() {
    return processFactory;
  }

  private ProcessInstanceSource processSource;

  private ProcessInstanceSource getProcessSource() {
    return processSource;
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

  public static ProcessInstanceSource getProcessSource(ProcessConfiguration processConfiguration) {
    if (processConfiguration.getProcessSource() != null) {
      return processConfiguration.getProcessSource();
    }
    if (processConfiguration.getProcessSourceName() != null) {
      try {
        Class cls = Class.forName(processConfiguration.getProcessSourceName());
        if (ProcessInstanceSource.class.isAssignableFrom(cls)) {
          ProcessInstanceSource source = ((ProcessInstanceSource) cls.newInstance());
          processConfiguration.setProcessSource(source);
          return source;
        }
      } catch (Exception ex) {
        throw new RuntimeException("Could not create process source", ex);
      }
    }
    throw new RuntimeException("Could not create process source");
  }

  public boolean isProcessSource() {
    return processSourceName != null || processSource != null;
  }
}
