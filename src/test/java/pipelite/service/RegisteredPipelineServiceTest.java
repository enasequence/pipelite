/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import pipelite.*;
import pipelite.exception.PipeliteException;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ContextConfiguration(initializers = PipeliteTestConfiguration.TestContextInitializer.class)
public class RegisteredPipelineServiceTest {

  @Autowired RegisteredPipelineService registeredPipelineService;

  @Autowired TestPipeline pipeline1;
  @Autowired TestPipeline pipeline2;

  @TestConfiguration
  public static class TestConfig {
    @Bean
    public TestPipeline pipeline1() {
      return new TestPipeline();
    }

    @Bean
    public TestPipeline pipeline2() {
      return new TestPipeline();
    }
  }

  public static class TestPipeline implements Pipeline {
    private final String pipelineName =
        UniqueStringGenerator.randomPipelineName(RegisteredPipelineServiceTest.class);

    @Override
    public int getPipelineParallelism() {
      return 1;
    }

    @Override
    public String getPipelineName() {
      return pipelineName;
    }

    @Override
    public Process createProcess(ProcessBuilder builder) {
      return null;
    }
  }

  @Test
  public void getRegisteredPipeline() {
    assertThat(
            registeredPipelineService
                .getRegisteredPipeline(pipeline1.getPipelineName())
                .getPipelineName())
        .isEqualTo(pipeline1.getPipelineName());
    assertThat(
            registeredPipelineService
                .getRegisteredPipeline(pipeline2.getPipelineName())
                .getPipelineName())
        .isEqualTo(pipeline2.getPipelineName());

    assertThat(
            registeredPipelineService
                .getRegisteredPipeline(pipeline1.getPipelineName(), Pipeline.class)
                .getPipelineName())
        .isEqualTo(pipeline1.getPipelineName());
    assertThat(
            registeredPipelineService
                .getRegisteredPipeline(pipeline2.getPipelineName(), Pipeline.class)
                .getPipelineName())
        .isEqualTo(pipeline2.getPipelineName());

    assertThat(
            registeredPipelineService.getRegisteredPipeline(
                pipeline1.getPipelineName(), Schedule.class))
        .isNull();
    assertThat(
            registeredPipelineService.getRegisteredPipeline(
                pipeline2.getPipelineName(), Schedule.class))
        .isNull();

    assertThatExceptionOfType(PipeliteException.class)
        .isThrownBy(() -> registeredPipelineService.getRegisteredPipeline(null))
        .withMessage("Missing pipeline name");
    assertThatExceptionOfType(PipeliteException.class)
        .isThrownBy(() -> registeredPipelineService.getRegisteredPipeline(""))
        .withMessage("Missing pipeline name");
    assertThatExceptionOfType(PipeliteException.class)
        .isThrownBy(
            () ->
                registeredPipelineService.getRegisteredPipeline(
                    UniqueStringGenerator.randomPipelineName(RegisteredPipelineServiceTest.class)))
        .withMessageStartingWith("Unknown pipeline");
  }
}
