package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.EmptyTestConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {"pipelite.launcher.launcherName=TEST", "pipelite.launcher.workers=1"})
@EnableConfigurationProperties(value = {LauncherConfiguration.class})
public class LauncherConfigurationTest {

  @Autowired LauncherConfiguration config;

  @Test
  public void test() {
    assertThat(config.getLauncherName()).isEqualTo("TEST");
    assertThat(config.getWorkers()).isEqualTo(1);
  }
}
