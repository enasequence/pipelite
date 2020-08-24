package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.EmptyTestConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {
      "pipelite.task.memory=1",
      "pipelite.task.cores=1",
      "pipelite.task.queue=TEST",
      "pipelite.task.memoryTimeout=15",
      "pipelite.task.retries=3",
      "pipelite.task.tempdir=",
      "pipelite.task.env=TEST1,TEST2"
    })
@EnableConfigurationProperties(value = {TaskConfiguration.class})
public class TaskConfigurationTest {

  @Autowired TaskConfiguration config;

  @Test
  public void test() {
    assertThat(config.getMemory()).isEqualTo(1);
    assertThat(config.getCores()).isEqualTo(1);
    assertThat(config.getQueue()).isEqualTo("TEST");
    assertThat(config.getMemoryTimeout()).isEqualTo(15);
    assertThat(config.getRetries()).isEqualTo(3);
    assertThat(config.getTempDir()).isBlank();
    assertThat(config.getEnv().length).isEqualTo(2);
    assertThat(config.getEnv()[0]).isEqualTo("TEST1");
    assertThat(config.getEnv()[1]).isEqualTo("TEST2");
  }
}
