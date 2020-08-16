package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.task.lsf")
public class LSFTaskExecutorConfiguration {

    public LSFTaskExecutorConfiguration() {
    }

    /**
     * Queue name.
     */
    public String queue;

    /**
     * Memory reservation timeout (minutes).
     */
     public Integer memoryTimeout;
}
