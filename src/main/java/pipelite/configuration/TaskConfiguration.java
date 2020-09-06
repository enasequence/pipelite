package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.executor.TaskExecutor;
import pipelite.task.ConfigurableTaskParameters;

import java.time.Duration;

import static pipelite.executor.PollableExecutor.DEFAULT_POLL_DELAY;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.task", ignoreInvalidFields = true)
/** Some configuration parameters are supported only by specific executors. */
public class TaskConfiguration implements ConfigurableTaskParameters {

  public TaskConfiguration() {}

  /** Remote host. */
  private String host;

  /** Execution timeout. */
  @Builder.Default private Duration timeout = TaskExecutor.DEFAULT_TIMEOUT;

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

  /** Delay between task polls. */
  @Builder.Default private Duration pollDelay = DEFAULT_POLL_DELAY;
}
