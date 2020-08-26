package pipelite.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.executor.TaskExecutorFactory;
import pipelite.stage.Stage;
import pipelite.stage.StageFactory;

@Component
public class ProcessConfigurationEx {

  private ProcessConfiguration processConfiguration;
  private TaskExecutorFactory executorFactory;
  private Stage[] stages;

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

  public void setStages(Stage[] stages) {
    this.stages = stages;
  }

  public Stage[] getStages() {
    if (stages != null) {
      return stages;
    }
    String stagesEnumName = processConfiguration.getStagesEnumName();
    String stagesFactoryName = processConfiguration.getStagesFactoryName();
    try {
      if (stagesEnumName != null) {
        return (Stage[]) loadEnum(stagesEnumName).getEnumConstants();
      }
      if (stagesFactoryName != null) {
        Class cls = Class.forName(stagesFactoryName);
        if (StageFactory.class.isAssignableFrom(cls)) {
          StageFactory stageFactory = ((StageFactory) cls.newInstance());
          return stageFactory.create();
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not retrieve process stages", ex);
    }
    throw new RuntimeException("Could not retrieve process stages");
  }

  private Class<? extends Enum<?>> loadEnum(String name) throws ClassNotFoundException {
    return ((Class<? extends Enum<?>>) Class.forName(name));
  }

  @Override
  public String toString() {
    return processConfiguration.toString();
  }
}
