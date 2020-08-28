package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.EmptyTestConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {"pipelite.process.processName=TEST"})
@EnableConfigurationProperties(value = {ProcessConfiguration.class})
public class ProcessConfigurationTest {

  @Autowired ProcessConfiguration config;

  @Test
  public void test() {
    assertThat(config.getProcessName()).isEqualTo("TEST");
  }
}
