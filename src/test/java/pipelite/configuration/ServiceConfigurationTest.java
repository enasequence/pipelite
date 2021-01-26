package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.PipeliteTestConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
public class ServiceConfigurationTest {

  @Autowired
  ServiceConfiguration serviceConfiguration;

  @Test
  public void getName() {
    String hostName = ServiceConfiguration.getCanonicalHostName();
    Integer port = ServiceConfiguration.DEFAULT_PORT;
    assertThat(serviceConfiguration.getName()).startsWith(hostName + ":" + port);
  }
}
