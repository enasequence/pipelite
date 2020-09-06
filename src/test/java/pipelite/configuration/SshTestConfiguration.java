package pipelite.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "pipelite.test.ssh")
public class SshTestConfiguration {
  private String host;
}
