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
import pipelite.TestConfiguration;

@SpringBootTest(
    classes = TestConfiguration.class,
    properties = {"pipelite.launcher.launcherName=TEST", "pipelite.launcher.workers=1"})
public class LauncherConfigurationTest {

  @Autowired LauncherConfiguration config;

  @Test
  public void test() {
    assertThat(config.getLauncherName()).isEqualTo("TEST");
    assertThat(config.getWorkers()).isEqualTo(1);
  }

  @Test
  public void getLauncherNameForPipeliteLauncherWithoutLauncherName() {
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    String pipelineName = "TEST1";
    String hostName = LauncherConfiguration.getHostName();
    assertThat(
            LauncherConfiguration.getLauncherNameForPipeliteLauncher(
                launcherConfiguration, pipelineName))
        .isEqualTo(hostName + "@" + pipelineName);
  }

  @Test
  public void getLauncherNameForPipeliteLauncherWithLauncherName() {
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setLauncherName("TEST2");
    String pipelineName = "TEST1";
    assertThat(
            LauncherConfiguration.getLauncherNameForPipeliteLauncher(
                launcherConfiguration, pipelineName))
        .isEqualTo("TEST2");
  }

  @Test
  public void getLauncherNameForPipeliteSchedulerWithoutLauncherName() {
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    String hostName = LauncherConfiguration.getHostName();
    String userName = LauncherConfiguration.getUserName();
    assertThat(LauncherConfiguration.getLauncherNameForPipeliteScheduler(launcherConfiguration))
        .isEqualTo(hostName + "@" + userName);
  }

  @Test
  public void getLauncherNameForPipeliteSchedulerWithLauncherName() {
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setLauncherName("TEST2");
    assertThat(LauncherConfiguration.getLauncherNameForPipeliteScheduler(launcherConfiguration))
        .isEqualTo("TEST2");
  }
}
