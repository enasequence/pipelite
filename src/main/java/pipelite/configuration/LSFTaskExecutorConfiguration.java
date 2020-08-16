package pipelite.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "pipelite.task.lsf")
public class LSFTaskExecutorConfiguration {

    /**
     * Queue name.
     */
    public String queue;

    /**
     * Memory reservation timeout (minutes).
     */
     public Integer memoryTimeout;
}
