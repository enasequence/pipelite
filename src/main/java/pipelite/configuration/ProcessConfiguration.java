package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;

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

  private ProcessFactory processFactory;

  private ProcessFactory getProcessFactory() {
    return processFactory;
  }

  private ProcessSource processSource;

  private ProcessSource getProcessSource() {
    return processSource;
  }

  public static ProcessFactory getProcessFactory(
      ProcessConfiguration processConfiguration) {
    if (processConfiguration.getProcessFactory() != null) {
      return processConfiguration.getProcessFactory();
    }
    if (processConfiguration.getProcessFactoryName() != null) {
      try {
        Class cls = Class.forName(processConfiguration.getProcessFactoryName());
        if (ProcessFactory.class.isAssignableFrom(cls)) {
          ProcessFactory factory = ((ProcessFactory) cls.newInstance());
          processConfiguration.setProcessFactory(factory);
          return factory;
        }
      } catch (Exception ex) {
        throw new RuntimeException("Could not create process factory", ex);
      }
    }
    throw new RuntimeException("Could not create process factory");
  }

  public static ProcessSource getProcessSource(ProcessConfiguration processConfiguration) {
    if (processConfiguration.getProcessSource() != null) {
      return processConfiguration.getProcessSource();
    }
    if (processConfiguration.getProcessSourceName() != null) {
      try {
        Class cls = Class.forName(processConfiguration.getProcessSourceName());
        if (ProcessSource.class.isAssignableFrom(cls)) {
          ProcessSource source = ((ProcessSource) cls.newInstance());
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
