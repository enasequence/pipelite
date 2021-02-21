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

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import pipelite.*;
import pipelite.configuration.ServiceConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.process.builder.ProcessBuilder;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

@SpringBootTest(classes = RegisteredPipelineServiceTest.TestConfig.class)
public class RegisteredPipelineServiceTest {

  @Autowired RegisteredPipelineService registeredPipelineService;
  @Autowired TestPipeline pipeline1;
  @Autowired TestPipeline pipeline2;
  @Autowired TestSchedule schedule1;
  @Autowired TestSchedule schedule2;
  @Autowired TestPrioritizedPipeline prioritizedPipeline1;
  @Autowired TestPrioritizedPipeline prioritizedPipeline2;

  public static class TestConfig {
    @Bean
    public RegisteredPipelineService registeredPipelineService(
        @Autowired List<RegisteredPipeline> registeredPipelines) {
      return new RegisteredPipelineService(
          mock(ServiceConfiguration.class), mock(ScheduleService.class), registeredPipelines);
    }

    @Bean
    public TestPipeline pipeline1() {
      return new TestPipeline();
    }

    @Bean
    public TestPipeline pipeline2() {
      return new TestPipeline();
    }

    @Bean
    public TestSchedule schedule1() {
      return new TestSchedule();
    }

    @Bean
    public TestSchedule schedule2() {
      return new TestSchedule();
    }

    @Bean
    public TestPrioritizedPipeline prioritizedPipeline1() {
      return new TestPrioritizedPipeline();
    }

    @Bean
    public TestPrioritizedPipeline prioritizedPipeline2() {
      return new TestPrioritizedPipeline();
    }
  }

  public static class TestPipeline implements Pipeline {
    private final String pipelineName =
        UniqueStringGenerator.randomPipelineName(RegisteredPipelineServiceTest.class);

    @Override
    public Options configurePipeline() {
      return new Options().pipelineParallelism(1);
    }

    @Override
    public String pipelineName() {
      return pipelineName;
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {}
  }

  public static class TestSchedule implements Schedule {
    private final String pipelineName =
        UniqueStringGenerator.randomPipelineName(RegisteredPipelineServiceTest.class);

    @Override
    public Options configurePipeline() {
      return new Options().cron("* * * * *");
    }

    @Override
    public String pipelineName() {
      return pipelineName;
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {}
  }

  public static class TestPrioritizedPipeline implements PrioritizedPipeline {
    private final String pipelineName =
        UniqueStringGenerator.randomPipelineName(RegisteredPipelineServiceTest.class);

    @Override
    public Options configurePipeline() {
      return new Options().pipelineParallelism(1);
    }

    @Override
    public String pipelineName() {
      return pipelineName;
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {}

    @Override
    public PrioritizedProcess nextProcess() {
      return null;
    }

    @Override
    public void confirmProcess(String processId) {}
  }

  private void assertGetRegisteredPipelineByName(
      RegisteredPipeline registeredPipeline, String pipelineName) {
    assertThat(registeredPipelineService.getRegisteredPipeline(pipelineName))
        .isEqualTo(registeredPipeline);
  }

  private <T extends RegisteredPipeline> void assertGetRegisteredPipelineByName(
      RegisteredPipeline registeredPipeline, Class<T> cls, String pipelineName) {
    assertThat(registeredPipelineService.getRegisteredPipeline(pipelineName, cls))
        .isEqualTo(registeredPipeline);
  }

  private <T extends RegisteredPipeline> void assertGetRegisteredPipelinesByType(
      List<T> registeredPipelines, Class<T> cls) {
    assertThat(registeredPipelineService.getRegisteredPipelines(cls))
        .containsExactlyInAnyOrderElementsOf(registeredPipelines);
  }

  @Test
  public void getRegisteredPipelineByName() {
    assertGetRegisteredPipelineByName(pipeline1, pipeline1.pipelineName());
    assertGetRegisteredPipelineByName(pipeline2, pipeline2.pipelineName());
    assertGetRegisteredPipelineByName(schedule1, schedule1.pipelineName());
    assertGetRegisteredPipelineByName(schedule2, schedule2.pipelineName());
    assertGetRegisteredPipelineByName(prioritizedPipeline1, prioritizedPipeline1.pipelineName());
    assertGetRegisteredPipelineByName(prioritizedPipeline2, prioritizedPipeline2.pipelineName());
  }

  @Test
  public void getRegisteredPipelineByNameAndType() {
    assertGetRegisteredPipelineByName(pipeline1, Pipeline.class, pipeline1.pipelineName());
    assertGetRegisteredPipelineByName(pipeline2, Pipeline.class, pipeline2.pipelineName());
    assertGetRegisteredPipelineByName(schedule1, Schedule.class, schedule1.pipelineName());
    assertGetRegisteredPipelineByName(schedule2, Schedule.class, schedule2.pipelineName());
    assertGetRegisteredPipelineByName(
        prioritizedPipeline1, PrioritizedPipeline.class, prioritizedPipeline1.pipelineName());
    assertGetRegisteredPipelineByName(
        prioritizedPipeline2, PrioritizedPipeline.class, prioritizedPipeline2.pipelineName());
  }

  @Test
  public void getRegisteredPipelineException() {
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

  @Test
  public void getRegisteredPipelinesByType() {
    assertGetRegisteredPipelinesByType(
        Arrays.asList(pipeline1, pipeline2, prioritizedPipeline1, prioritizedPipeline2),
        Pipeline.class);
    assertGetRegisteredPipelinesByType(Arrays.asList(schedule1, schedule2), Schedule.class);
    assertGetRegisteredPipelinesByType(
        Arrays.asList(prioritizedPipeline1, prioritizedPipeline2), PrioritizedPipeline.class);
  }

  @Test
  public void isScheduler() {
    assertThat(registeredPipelineService.isScheduler()).isTrue();
  }
}
