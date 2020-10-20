package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import pipelite.PipeliteTestConfiguration;
import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.process.ProcessFactory;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
public class ProcessFactoryServiceTest {

  @Autowired ProcessFactoryService processFactoryService;

  private static final String PIPELINE_NAME_1 = UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_2 = UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_3 = UniqueStringGenerator.randomPipelineName();

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory first() {
      return new TestInMemoryProcessFactory(PIPELINE_NAME_1, Collections.emptyList());
    }

    @Bean
    public ProcessFactory second() {
      return new TestInMemoryProcessFactory(PIPELINE_NAME_2, Collections.emptyList());
    }
  }

  @Test
  public void test() {
    assertThat(processFactoryService.create(PIPELINE_NAME_1).getPipelineName())
        .isEqualTo(PIPELINE_NAME_1);
    assertThat(processFactoryService.create(PIPELINE_NAME_2).getPipelineName())
        .isEqualTo(PIPELINE_NAME_2);
    assertThatExceptionOfType(ProcessFactoryServiceException.class)
        .isThrownBy(() -> processFactoryService.create(null))
        .withMessage("Missing pipeline name");
    assertThatExceptionOfType(ProcessFactoryServiceException.class)
        .isThrownBy(() -> processFactoryService.create(""))
        .withMessage("Missing pipeline name");
    assertThatExceptionOfType(ProcessFactoryServiceException.class)
        .isThrownBy(() -> processFactoryService.create(PIPELINE_NAME_3))
        .withMessageStartingWith("Unknown pipeline");
  }
}
