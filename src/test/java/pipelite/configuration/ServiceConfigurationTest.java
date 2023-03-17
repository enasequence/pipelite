/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithConfigurations;

@SpringBootTest(classes = PipeliteTestConfigWithConfigurations.class)
@ActiveProfiles("test")
public class ServiceConfigurationTest {

  @Autowired ServiceConfiguration serviceConfiguration;

  @Test
  public void getName() {
    String hostName = ServiceConfiguration.getCanonicalHostName();
    int port = ServiceConfiguration.DEFAULT_PORT;
    assertThat(serviceConfiguration.getName()).isEqualTo(hostName + ":" + port);
  }

  @Test
  public void getUrl() {
    String hostName = ServiceConfiguration.getCanonicalHostName();
    int port = ServiceConfiguration.DEFAULT_PORT;
    String contextPath =
        serviceConfiguration.getContextPath().replaceAll("^/*", "").replaceAll("/*$", "");
    assertThat(serviceConfiguration.getUrl()).isEqualTo(hostName + ":" + port + "/" + contextPath);
  }
}
