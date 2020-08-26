package pipelite.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.executor.TaskExecutorFactory;
import pipelite.instance.ProcessInstanceFactory;

@Component
public class ProcessConfigurationEx {

  private ProcessConfiguration processConfiguration;
  private TaskExecutorFactory executorFactory;
  private ProcessInstanceFactory processFactory;

  public ProcessConfigurationEx(@Autowired ProcessConfiguration processConfiguration) {
    this.processConfiguration = processConfiguration;
  }

  public void setProcessConfiguration(ProcessConfiguration processConfiguration) {
    this.processConfiguration = processConfiguration;
  }

  public ProcessConfiguration getProcessConfiguration() {
    return processConfiguration;
  }

  public void setProcessName(String processName) {
    processConfiguration.setProcessName(processName);
  }

  public String getProcessName() {
    return processConfiguration.getProcessName();
  }

  public void setExecutorFactory(TaskExecutorFactory executorFactory) {
    this.executorFactory = executorFactory;
  }

  public TaskExecutorFactory getExecutorFactory() {
    if (executorFactory != null) {
      return executorFactory;
    }
    try {
      return ((TaskExecutorFactory)
          Class.forName(processConfiguration.getExecutorFactoryName()).newInstance());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void setProcessFactory(ProcessInstanceFactory processFactory) {
    this.processFactory = processFactory;
  }

  public ProcessInstanceFactory getProcessFactory() {
    if (processFactory != null) {
      return processFactory;
    }
    String processFactoryName = processConfiguration.getProcessFactoryName();
    if (processFactoryName != null) {
      try {
        Class cls = Class.forName(processFactoryName);
        if (ProcessInstanceFactory.class.isAssignableFrom(cls)) {
          return ((ProcessInstanceFactory) cls.newInstance());
        }
      } catch (Exception ex) {
        throw new RuntimeException("Could not create process factory", ex);
      }
    }

    throw new RuntimeException("Could not create process factory");
  }

  @Override
  public String toString() {
    return processConfiguration.toString();
  }
}
