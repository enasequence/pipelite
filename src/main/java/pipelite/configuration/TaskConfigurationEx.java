package pipelite.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.resolver.ExceptionResolver;
import pipelite.task.TaskFactory;

@Component
public class TaskConfigurationEx implements TaskParameters {

  private TaskConfiguration taskConfiguration;
  private TaskFactory taskFactory;
  private ExceptionResolver resolver;

  public TaskConfigurationEx(@Autowired TaskConfiguration taskConfiguration) {
    this.taskConfiguration = taskConfiguration;
  }

  public TaskConfiguration getTaskConfiguration() {
    return taskConfiguration;
  }

  public void setTaskConfiguration(TaskConfiguration taskConfiguration) {
    this.taskConfiguration = taskConfiguration;
  }

  public Integer getMemory() {
    return taskConfiguration.getMemory();
  }

  public void setMemory(Integer memory) {
    taskConfiguration.setMemory(memory);
  }

  public Integer getMemoryTimeout() {
    return taskConfiguration.getMemoryTimeout();
  }

  public void setMemoryTimeout(Integer memoryTimeout) {
    taskConfiguration.setMemoryTimeout(memoryTimeout);
  }

  public Integer getCores() {
    return taskConfiguration.getCores();
  }

  public void setCores(Integer cores) {
    taskConfiguration.setCores(cores);
  }

  public String getQueue() {
    return taskConfiguration.getQueue();
  }

  public void setQueue(String queue) {
    taskConfiguration.setQueue(queue);
  }

  public Integer getRetries() {
    return taskConfiguration.getRetries();
  }

  public void setRetries(Integer retries) {
    taskConfiguration.setRetries(retries);
  }

  public String getTempDir() {
    return taskConfiguration.getTempDir();
  }

  public void setTempDir(String tempDir) {
    taskConfiguration.setTempDir(tempDir);
  }

  public String[] getEnv() {
    return taskConfiguration.getEnv();
  }

  public void setEnv(String[] env) {
    taskConfiguration.setEnv(env);
  }

  public void setTaskFactory(TaskFactory taskFactory) {
    this.taskFactory = taskFactory;
  }

  public TaskFactory getTaskFactory() {
    if (taskFactory != null) {
      return taskFactory;
    }
    try {
      return ((TaskFactory) Class.forName(taskConfiguration.getTaskFactoryName()).newInstance());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void setResolver(ExceptionResolver resolver) {
    this.resolver = resolver;
  }

  public ExceptionResolver getResolver() {
    if (resolver != null) {
      return resolver;
    }
    try {
      return (ExceptionResolver) Class.forName(taskConfiguration.getResolver()).newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public String toString() {
    return taskConfiguration.toString();
  }
}
