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
import pipelite.PipeliteTestConfiguration;
import pipelite.ProcessSource;
import pipelite.TestProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.exception.PipeliteException;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
public class RegisteredProcessSourceServiceTest {

  @Autowired RegisteredProcessSourceService registeredProcessSourceService;

  private static final String PIPELINE_NAME_1 =
      UniqueStringGenerator.randomPipelineName(RegisteredProcessSourceServiceTest.class);
  private static final String PIPELINE_NAME_2 =
      UniqueStringGenerator.randomPipelineName(RegisteredProcessSourceServiceTest.class);
  private static final String PIPELINE_NAME_3 =
      UniqueStringGenerator.randomPipelineName(RegisteredProcessSourceServiceTest.class);

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessSource firstProcessSource() {
      return new TestProcessSource(PIPELINE_NAME_1, 0);
    }

    @Bean
    public ProcessSource secondProcessSource() {
      return new TestProcessSource(PIPELINE_NAME_2, 0);
    }
  }

  @Test
  public void test() {
    assertThat(registeredProcessSourceService.create(PIPELINE_NAME_1).getPipelineName())
        .isEqualTo(PIPELINE_NAME_1);
    assertThat(registeredProcessSourceService.create(PIPELINE_NAME_2).getPipelineName())
        .isEqualTo(PIPELINE_NAME_2);
    assertThat(registeredProcessSourceService.create(PIPELINE_NAME_3)).isNull();
    assertThatExceptionOfType(PipeliteException.class)
        .isThrownBy(() -> registeredProcessSourceService.create(null))
        .withMessage("Missing pipeline name");
    assertThatExceptionOfType(PipeliteException.class)
        .isThrownBy(() -> registeredProcessSourceService.create(""))
        .withMessage("Missing pipeline name");
  }
}
