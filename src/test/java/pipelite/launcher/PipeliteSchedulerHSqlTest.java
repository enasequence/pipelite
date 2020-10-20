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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;

@SpringBootTest(
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.launcher.processLaunchFrequency=250ms",
      "pipelite.launcher.stageLaunchFrequency=250ms",
      "pipelite.stage.maximumRetries=1"
    })
@ContextConfiguration(initializers = PipeliteSchedulerHSqlTest.TestContextInitializer.class)
@ActiveProfiles(value = {"hsql-test"})
public class PipeliteSchedulerHSqlTest {

  @Autowired private ObjectProvider<PipeliteSchedulerTester> tester;

  public static class TestContextInitializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues.of(
              "pipelite.launcher.launcherName=" + UniqueStringGenerator.randomLauncherName())
          .applyTo(configurableApplicationContext.getEnvironment());
    }
  }

  @Test
  public void testOneProcess() {
    tester.getObject().testOneProcess();
  }

  @Test
  public void testThreeProcesses() {
    tester.getObject().testThreeProcesses();
  }

  @Test
  public void testOneProcessOneFailure() {
    tester.getObject().testOneProcessesOneFailure();
  }

  @Test
  public void testThreeProcessesOneFailure() {
    tester.getObject().testThreeProcessesOneFailure();
  }

  @Test
  public void testThreeProcessesAllFailure() {
    tester.getObject().testThreeProcessesAllFailure();
  }
}
