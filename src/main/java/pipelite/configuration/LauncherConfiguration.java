package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.ForkJoinPool;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.pipelite.launcher")
public class LauncherConfiguration {

  public LauncherConfiguration() {}

  /** Name of the pipelite.launcher begin executed. */
  private String launcherName;

  /** Number of parallel task executions. */
  @Builder.Default
  private int workers = ForkJoinPool.getCommonPoolParallelism();
}
