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
package pipelite.executor.service;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.PipeliteTestConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("aws-test")
@SpringBootTest(classes = PipeliteTestConfiguration.class)
public class AwsBatchExecutorServiceTest {

  @Autowired AwsBatchExecutorService service;

  @Test
  @Disabled
  public void client() {
    assertThat(service.client("region1")).isSameAs(service.client("region1"));
    assertThat(service.client("region2")).isSameAs(service.client("region2"));
    assertThat(service.client("region1")).isNotSameAs(service.client("region2"));
    assertThat(service.client(null)).isSameAs(service.client(null));
    assertThat(service.client(null)).isNotSameAs(service.client("region1"));
  }
}
