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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.*;
import pipelite.exception.PipeliteException;
import pipelite.helper.ConfigureProcessPipelineTestHelper;
import pipelite.helper.ScheduleTestHelper;
import pipelite.process.builder.ProcessBuilder;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=RegisteredPipelineServiceTest"
    })
@DirtiesContext
@ActiveProfiles({"test", "RegisteredPipelineServiceTest"})
public class RegisteredPipelineServiceTest {

  @Autowired RegisteredPipelineService registeredPipelineService;
  @Autowired TestPipeline pipeline1;
  @Autowired TestPipeline pipeline2;
  @Autowired TestSchedule schedule1;
  @Autowired TestSchedule schedule2;

  @TestConfiguration
  @Profile("RegisteredPipelineServiceTest")
  public static class TestConfig {
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
  }

  public static class TestPipeline extends ConfigureProcessPipelineTestHelper {

    @Override
    public int testConfigureParallelism() {
      return 1;
    }

    @Override
    public void testConfigureProcess(ProcessBuilder builder) {}
  }

  public static class TestSchedule extends ScheduleTestHelper {
    public TestSchedule() {
      super(PipeliteTestConstants.CRON_EVERY_TWO_SECONDS);
    }

    @Override
    public void testConfigureProcess(ProcessBuilder builder) {}
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
  }

  @Test
  public void getRegisteredPipelineByNameAndType() {
    assertGetRegisteredPipelineByName(pipeline1, Pipeline.class, pipeline1.pipelineName());
    assertGetRegisteredPipelineByName(pipeline2, Pipeline.class, pipeline2.pipelineName());
    assertGetRegisteredPipelineByName(schedule1, Schedule.class, schedule1.pipelineName());
    assertGetRegisteredPipelineByName(schedule2, Schedule.class, schedule2.pipelineName());
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
    assertGetRegisteredPipelinesByType(Arrays.asList(pipeline1, pipeline2), Pipeline.class);
    assertGetRegisteredPipelinesByType(Arrays.asList(schedule1, schedule2), Schedule.class);
  }

  @Test
  public void isSchedules() {
    assertThat(registeredPipelineService.isSchedules()).isTrue();
  }
}
