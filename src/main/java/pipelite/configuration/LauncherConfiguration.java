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
  private int workers;

  /** Delay between launcher runs. */
  private Duration runDelay = PipeliteLauncher.DEFAULT_RUN_DELAY;

  /** Delay before launcher stops. */
  private Duration stopDelay = PipeliteLauncher.DEFAULT_STOP_DELAY;
}
