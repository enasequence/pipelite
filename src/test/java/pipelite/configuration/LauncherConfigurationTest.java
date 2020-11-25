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
package pipelite.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;

@SpringBootTest(
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.launcher.schedulerName=TEST",
      "pipelite.launcher.pipelineParallelism=1"
    })
@ActiveProfiles(value = {"hsql-test"})
public class LauncherConfigurationTest {

  @Autowired LauncherConfiguration config;

  @Test
  public void testPipelineParallelism() {
    assertThat(config.getPipelineParallelism()).isEqualTo(1);
  }

  @Test
  public void getSchedulerName() {
    assertThat(config.getSchedulerName()).isEqualTo("TEST");
    assertThat(LauncherConfiguration.getSchedulerName(config)).isEqualTo("TEST");
  }

  @Test
  public void getLauncherName() {
    String hostName = LauncherConfiguration.getHostName();
    String userName = LauncherConfiguration.getUserName();
    assertThat(LauncherConfiguration.getLauncherName("TEST", 8080))
        .isEqualTo(hostName + ":8080@" + "TEST");
  }
}
