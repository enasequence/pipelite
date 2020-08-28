package pipelite.instance;

import lombok.Builder;
import lombok.Data;
import pipelite.configuration.TaskConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.resolver.TaskExecutionResultResolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class TaskParameters {


  private Integer retries;
  private String[] env;
  private String tempDir;
  private Integer memory;
  private Integer memoryTimeout;
  private Integer cores;
  private String queue;

  /* Add parameters. Existing values will not be replaced. */
  public void add(TaskConfiguration taskConfiguration) {
    if (taskConfiguration == null) {
      return;
    }
    setRetries(TaskParametersUtils.getRetries(this, taskConfiguration));
    setEnv(TaskParametersUtils.getEnv(this, taskConfiguration));
    setTempDir(TaskParametersUtils.getTempDir(this, taskConfiguration));
    setMemory(TaskParametersUtils.getMemory(this, taskConfiguration));
    setMemoryTimeout(TaskParametersUtils.getMemoryTimeout(this, taskConfiguration));
    setCores(TaskParametersUtils.getCores(this, taskConfiguration));
    setQueue(TaskParametersUtils.getQueue(this, taskConfiguration));
  }

  public List<String> getEnvAsJavaSystemPropertyOptions() {
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

  public Map<String, String> getEnvAsMap() {
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
