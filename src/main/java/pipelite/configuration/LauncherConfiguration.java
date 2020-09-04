package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.entity.PipeliteProcess;
import pipelite.launcher.PipeliteLauncher;

import java.time.Duration;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.launcher")
public class LauncherConfiguration {

  public LauncherConfiguration() {}

  /** Name of the pipelite.launcher begin executed. */
  private String launcherName;

  /** Number of parallel task executions. */
  @Builder.Default private int workers = PipeliteLauncher.DEFAULT_WORKERS;

  /** Delay between launcher runs. */
  @Builder.Default private Duration runDelay = PipeliteLauncher.DEFAULT_RUN_DELAY;

  /** Delay before launcher stops. */
  @Builder.Default private Duration stopDelay = PipeliteLauncher.DEFAULT_STOP_DELAY;

  /** Delay before launcher allows new high priority tasks to take precedence. */
  @Builder.Default private Duration priorityDelay = PipeliteLauncher.DEFAULT_PRIORITY_DELAY;
}
