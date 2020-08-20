package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.launcher")
public class LauncherConfiguration {

  public LauncherConfiguration() {}

  /** Name of the launcher begin executed. */
  private String launcherName;

  /** Number of parallel task executions. */
  private int workers;
}
