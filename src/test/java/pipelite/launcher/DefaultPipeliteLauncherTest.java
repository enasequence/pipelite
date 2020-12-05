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
package pipelite.launcher;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import pipelite.configuration.LauncherConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultPipeliteLauncherTest {

  @Autowired
  @Test
  public void test() {
    LauncherConfiguration config = new LauncherConfiguration();
    config.setPort(8080);
    String hostName = DefaultPipeliteLauncher.getCanonicalHostName();
    assertThat(DefaultPipeliteLauncher.launcherName("TEST", config))
        .startsWith("TEST@" + hostName + ":8080:");
  }
}
