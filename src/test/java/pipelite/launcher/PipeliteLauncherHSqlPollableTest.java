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
import pipelite.TestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.service.LockService;
import pipelite.service.ProcessService;
import pipelite.service.StageService;

@SpringBootTest(
    classes = TestConfiguration.class,
    properties = {
      "pipelite.launcher.workers=2",
      "pipelite.launcher.processLaunchFrequency=250ms",
      "pipelite.launcher.stageLaunchFrequency=250ms",
      "pipelite.stage.retries=1"
    })
@ContextConfiguration(initializers = PipeliteLauncherHSqlSuccessTest.TestContextInitializer.class)
@ActiveProfiles(value = {"hsql-test"})
public class PipeliteLauncherHSqlPollableTest {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private StageConfiguration stageConfiguration;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private LockService lockService;

  public PipeliteLauncherHSqlPollableTest() {}

  public static class TestContextInitializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues.of(
              "pipelite.launcher.launcherName=" + UniqueStringGenerator.randomLauncherName(),
              "pipelite.process.pipelineName=" + UniqueStringGenerator.randomPipelineName())
          .applyTo(configurableApplicationContext.getEnvironment());
    }
  }

  @Test
  public void testPollSuccessExecuteSuccess() {
    PipeliteLauncherPollableTester tester =
        new PipeliteLauncherPollableTester(
            launcherConfiguration,
            processConfiguration,
            stageConfiguration,
            processService,
            stageService,
            lockService);
    tester.testPollSuccessExecuteSuccess();
  }

  @Test
  public void testPollErrorExecuteSuccess() {
    PipeliteLauncherPollableTester tester =
        new PipeliteLauncherPollableTester(
            launcherConfiguration,
            processConfiguration,
            stageConfiguration,
            processService,
            stageService,
            lockService);
    tester.testPollErrorExecuteSuccess();
  }

  @Test
  public void testPollErrorExecuteError() {
    PipeliteLauncherPollableTester tester =
        new PipeliteLauncherPollableTester(
            launcherConfiguration,
            processConfiguration,
            stageConfiguration,
            processService,
            stageService,
            lockService);
    tester.testPollErrorExecuteSuccess();
  }

  @Test
  public void testPollExceptionExecuteSuccess() {
    PipeliteLauncherPollableTester tester =
        new PipeliteLauncherPollableTester(
            launcherConfiguration,
            processConfiguration,
            stageConfiguration,
            processService,
            stageService,
            lockService);

    tester.testPollExceptionExecuteSuccess();
  }

  @Test
  public void testPollExceptionExecuteError() {
    PipeliteLauncherPollableTester tester =
        new PipeliteLauncherPollableTester(
            launcherConfiguration,
            processConfiguration,
            stageConfiguration,
            processService,
            stageService,
            lockService);

    tester.testPollExceptionExecuteError();
  }
}
