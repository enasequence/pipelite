package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import pipelite.PipeliteTestConfiguration;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.process.ProcessSource;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
public class ProcessSourceServiceTest {

  @Autowired ProcessSourceService processSourceService;

  private static final String PIPELINE_NAME_1 = UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_2 = UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_3 = UniqueStringGenerator.randomPipelineName();

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessSource firstProcessSource() {
      return new TestInMemoryProcessSource(PIPELINE_NAME_1, Collections.emptyList());
    }

    @Bean
    public ProcessSource secondProcessSource() {
      return new TestInMemoryProcessSource(PIPELINE_NAME_2, Collections.emptyList());
    }
  }

  @Test
  public void test() {
    assertThat(processSourceService.create(PIPELINE_NAME_1).getPipelineName())
        .isEqualTo(PIPELINE_NAME_1);
    assertThat(processSourceService.create(PIPELINE_NAME_2).getPipelineName())
        .isEqualTo(PIPELINE_NAME_2);
    assertThat(processSourceService.create(PIPELINE_NAME_3)).isNull();
    assertThatExceptionOfType(ProcessSourceServiceException.class)
        .isThrownBy(() -> processSourceService.create(null))
        .withMessage("Missing pipeline name");
    assertThatExceptionOfType(ProcessSourceServiceException.class)
        .isThrownBy(() -> processSourceService.create(""))
        .withMessage("Missing pipeline name");
  }
}
