package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.task")
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
  public int retries;

  /** Temporary directory. */
  public String tempDir;

  /** Environmental variables. */
  private String[] env;

}
