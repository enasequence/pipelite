package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.task")
public class TaskExecutorConfiguration {

    public TaskExecutorConfiguration() {
    }

    /**
     * Default amount of memory (MBytes) requested for task execution.
     */
    public int memory;

    /**
     * Default number of CPUs requested for task execution.
     */
    public int cores;

    /**
     * Default number of retries.
     */
    public int retries;

    /**
     * Temporary directory.
     */
    public String tempDir;
}
