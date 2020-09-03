package pipelite.task;

import lombok.Builder;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.TaskConfiguration;
import pipelite.json.Json;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@Flogger
public class TaskParameters {

  private String host; // Used by SshSystemCallExecutor.
  private Duration timeout;
  private Integer retries;
  private String[] env;
  private String workDir;
  private Integer memory;
  private Duration memoryTimeout;
  private Integer cores;
  private String queue;
  private Duration pollFrequency;

  /* Add parameters. Existing values will not be replaced. */
  public void add(TaskConfiguration taskConfiguration) {
    if (taskConfiguration == null) {
      return;
    }
    setHost(TaskParametersUtils.getHost(this, taskConfiguration));
    setTimeout(TaskParametersUtils.getTimeout(this, taskConfiguration));
    setRetries(TaskParametersUtils.getRetries(this, taskConfiguration));
    setEnv(TaskParametersUtils.getEnv(this, taskConfiguration));
    setWorkDir(TaskParametersUtils.getTempDir(this, taskConfiguration));
    setMemory(TaskParametersUtils.getMemory(this, taskConfiguration));
    setMemoryTimeout(TaskParametersUtils.getMemoryTimeout(this, taskConfiguration));
    setCores(TaskParametersUtils.getCores(this, taskConfiguration));
    setQueue(TaskParametersUtils.getQueue(this, taskConfiguration));
    setPollFrequency(TaskParametersUtils.getPollFrequency(this, taskConfiguration));
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
    Map<String, String> options = new HashMap<>();
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

  public String json() {
    return Json.serializeNullIfErrorOrEmpty(this);
  }
}
