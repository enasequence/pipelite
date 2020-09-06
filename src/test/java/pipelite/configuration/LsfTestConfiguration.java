package pipelite.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "pipelite.test.lsf")
public class LsfTestConfiguration {
  private String host;
}
