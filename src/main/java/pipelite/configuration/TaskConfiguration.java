package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.task", ignoreInvalidFields = true)
/** Some configuration parameters are supported only by specific executors. */
public class TaskConfiguration {

  public TaskConfiguration() {}

  /** Remote host. */
  private String host;

  /** Execution timeout. */
  private Duration timeout;

  /** Memory reservation (MBytes). */
  private Integer memory;

  /** Memory reservation timeout (minutes). */
  private Duration memoryTimeout;

  /** Core reservation. */
  private Integer cores;

  /** Queue name. */
  private String queue;

  /** Number of retries. */
  private Integer retries;

  /** Work directory. */
  private String workDir;

  /** Environmental variables. */
  private String[] env;

  /** Name of the resolver class for task execution results. */
  private String resolver;

  private Duration pollFrequency;
}
