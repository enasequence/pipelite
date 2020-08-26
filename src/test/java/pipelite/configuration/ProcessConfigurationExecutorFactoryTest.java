package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import pipelite.EmptyTestConfiguration;
import pipelite.executor.LsfTaskExecutorFactory;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {"pipelite.process.executorFactoryName=pipelite.executor.LsfTaskExecutorFactory"})
@EnableConfigurationProperties(
    value = {LauncherConfiguration.class, ProcessConfiguration.class, TaskConfiguration.class})
@ComponentScan(basePackageClasses = {ProcessConfigurationEx.class})
public class ProcessConfigurationExecutorFactoryTest {

  @Autowired ProcessConfigurationEx processConfiguration;

  @Test
  public void test() {
    assertThat(processConfiguration.getExecutorFactory())
        .isInstanceOf(LsfTaskExecutorFactory.class);
  }
}
