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
public class TaskParameters implements ConfigurableTaskParameters {

  private String host; // Executors: SSH
  private Duration timeout; // Executors: all
  private Integer retries; // Executors: all
  private String[] env; // Executors: all
  private String workDir; // Executors: LSF
  private Integer memory; // Executors: LSF
  private Duration memoryTimeout; // Executors: LSF
  private Integer cores; // Executors: LSF
  private String queue; // Executors: LSF
  private Duration pollDelay; // Executors: LSF

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
    setPollDelay(TaskParametersUtils.getPollDelay(this, taskConfiguration));
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
