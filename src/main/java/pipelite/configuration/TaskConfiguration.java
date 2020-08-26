package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.instance.TaskParameters;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.task", ignoreInvalidFields = true)
/** Some configuration parameters are supported only by specific executors. */
public class TaskConfiguration implements TaskParameters {

  public TaskConfiguration() {}

  /** Memory reservation (MBytes). */
  public Integer memory;

  /** Memory reservation timeout (minutes). */
  public Integer memoryTimeout;

  /** Core reservation. */
  public Integer cores;

  /** Queue name. */
  public String queue;

  /** Number of retries. */
  public Integer retries;

  /** Temporary directory. */
  public String tempDir;

  /** Environmental variables. */
  private String[] env;

  /** Name of the resolver class for task execution results. */
  private String resolver;

  /** Allow the task factory to be defined by name. */
  private String taskFactoryName;
}
