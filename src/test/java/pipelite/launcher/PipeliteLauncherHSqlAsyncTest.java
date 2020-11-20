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
      "pipelite.launcher.pipelineParallelism=2",
      "pipelite.launcher.processLaunchFrequency=250ms",
      "pipelite.launcher.stageLaunchFrequency=250ms",
      "pipelite.launcher.shutdownIfIdle=true",
      "pipelite.stage.maximumRetries=1"
    })
@ContextConfiguration(initializers = PipeliteLauncherHSqlSuccessTest.TestContextInitializer.class)
@ActiveProfiles(value = {"hsql-test"})
public class PipeliteLauncherHSqlAsyncTest {

  @Autowired private ObjectProvider<PipeliteLauncherAsyncTester> testerObjectProvider;

  public PipeliteLauncherHSqlAsyncTest() {}

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
  public void testSubmitSuccessPollSuccess() {
    testerObjectProvider.getObject().testSubmitSuccessPollSuccess();
  }

  @Test
  public void testSubmitError() {
    testerObjectProvider.getObject().testSubmitError();
  }

  @Test
  public void testSubmitException() {
    testerObjectProvider.getObject().testSubmitException();
  }

  @Test
  public void testPollError() {
    testerObjectProvider.getObject().testPollError();
  }

  @Test
  public void testPollException() {
    testerObjectProvider.getObject().testPollException();
  }
}