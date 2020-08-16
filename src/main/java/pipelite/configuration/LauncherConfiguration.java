package pipelite.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "pipelite.launcher")
public class LauncherConfiguration {
    /**
     * Name of the process begin executed.
     */
    private String processName;

    /**
     * Number of parallel task executions.
     */
    private int workers;
}
