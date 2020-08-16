package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.process")
public class ProcessConfiguration {

    public ProcessConfiguration() {
    }

    /**
     * Name of the class that resolves results.
     */
    private String resolver;
}
