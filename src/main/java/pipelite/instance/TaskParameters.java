package pipelite.instance;

import pipelite.resolver.TaskExecutionResultResolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface TaskParameters {

  TaskExecutionResultResolver getResolver();

  void setResolver(TaskExecutionResultResolver resolver);

  Integer getRetries();

  void setRetries(Integer retries);

  String[] getEnv();

  void setEnv(String[] env);

  String getTempDir();

  void setTempDir(String tempDir);

  Integer getMemory();

  void setMemory(Integer memory);

  Integer getMemoryTimeout();

  void setMemoryTimeout(Integer memoryTimeout);

  Integer getCores();

  void setCores(Integer cores);

  String getQueue();

  void setQueue(String queue);

  /* Add parameters. Existing values will not be replaced. */
  default void add(TaskParameters taskParameters) {
    if (taskParameters == null) {
      return;
    }
    setResolver(TaskParametersUtils.getResolver(this, taskParameters));
    setRetries(TaskParametersUtils.getRetries(this, taskParameters));
    setEnv(TaskParametersUtils.getEnv(this, taskParameters));
    setTempDir(TaskParametersUtils.getTempDir(this, taskParameters));
    setMemory(TaskParametersUtils.getMemory(this, taskParameters));
    setMemoryTimeout(TaskParametersUtils.getMemoryTimeout(this, taskParameters));
    setCores(TaskParametersUtils.getCores(this, taskParameters));
    setQueue(TaskParametersUtils.getQueue(this, taskParameters));
  }

  default List<String> getEnvAsJavaSystemPropertyOptions() {
    List<String> options = new ArrayList<>();
    if (getEnv() != null) {
      for (String property : getEnv()) {
        String value = System.getProperty(property);
        if (value != null) {
          options.add(String.format("-D%s=%s", property, value));
        }
      }
    }
    return options;
  }

  default Map<String, String> getEnvAsMap() {
    Map<String, String> options = new HashMap();
    if (getEnv() != null) {
      for (String property : getEnv()) {
        String value = System.getProperty(property);
        if (value != null) {
          options.put(property, value);
        }
      }
    }
    return options;
  }
}
