package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import pipelite.EmptyTestConfiguration;
import pipelite.resolver.DefaultExceptionResolver;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {"pipelite.task.resolver=pipelite.resolver.DefaultExceptionResolver"})
@EnableConfigurationProperties(
    value = {LauncherConfiguration.class, ProcessConfiguration.class, TaskConfiguration.class})
@ComponentScan(basePackageClasses = {TaskConfigurationEx.class})
public class TaskConfigurationResolverTest {

  @Autowired TaskConfigurationEx config;

  @Test
  public void test() {
    assertThat(config.getResolver()).isInstanceOf(DefaultExceptionResolver.class);
  }
}
