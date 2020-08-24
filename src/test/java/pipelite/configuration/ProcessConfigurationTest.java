package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.EmptyTestConfiguration;
import pipelite.executor.LsfTaskExecutorFactory;
import pipelite.resolver.DefaultExceptionResolver;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {
      "pipelite.process.processName=TEST",
      "pipelite.process.executor=pipelite.executor.LsfTaskExecutorFactory",
      "pipelite.process.resolver=pipelite.resolver.DefaultExceptionResolver"
    })
@EnableConfigurationProperties(value = {ProcessConfiguration.class})
public class ProcessConfigurationTest {

  @Autowired ProcessConfiguration config;

  @Test
  public void test() {
    assertThat(config.getProcessName()).isEqualTo("TEST");
    assertThat(config.getExecutor()).isEqualTo("pipelite.executor.LsfTaskExecutorFactory");
    assertThat(config.createExecutorFactory()).isInstanceOf(LsfTaskExecutorFactory.class);
    assertThat(config.getResolver()).isEqualTo("pipelite.resolver.DefaultExceptionResolver");
    assertThat(config.createResolver()).isInstanceOf(DefaultExceptionResolver.class);
  }
}
