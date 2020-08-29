package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.test")
public class TestConfiguration {

  public TestConfiguration() {}

  @NestedConfigurationProperty
  private Ssh ssh;

  @Data
  public static class Ssh {

    private String host;
  }
}
