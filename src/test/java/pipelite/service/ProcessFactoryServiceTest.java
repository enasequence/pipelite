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

import java.util.Collections;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;
import pipelite.TestProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.process.ProcessFactory;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
public class ProcessFactoryServiceTest {

  @Autowired ProcessFactoryService processFactoryService;

  private static final String PIPELINE_NAME_1 = UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_2 = UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_3 = UniqueStringGenerator.randomPipelineName();

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory firstProcessFactory() {
      return new TestProcessFactory(PIPELINE_NAME_1, Collections.emptyList());
    }

    @Bean
    public ProcessFactory secondProcessFactory() {
      return new TestProcessFactory(PIPELINE_NAME_2, Collections.emptyList());
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
