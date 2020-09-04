package pipelite.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@Data
@ConfigurationProperties(prefix = "pipelite.test")
public class TestConfiguration {

  public TestConfiguration() {}

  @NestedConfigurationProperty private Ssh ssh;

  @NestedConfigurationProperty private Lsf lsf;

  @Data
  public static class Ssh {
    private String host;
  }

  @Data
  public static class Lsf {
    private String host;
  }
}
