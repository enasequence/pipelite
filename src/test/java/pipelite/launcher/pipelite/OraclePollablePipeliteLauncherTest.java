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
package pipelite.launcher.pipelite;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import pipelite.FullTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.service.LockService;
import pipelite.service.ProcessService;
import pipelite.service.TaskService;

@SpringBootTest(
    classes = FullTestConfiguration.class,
    properties = {
      "pipelite.launcher.workers=2",
      "pipelite.launcher.processLaunchFrequency=250ms",
      "pipelite.launcher.taskLaunchFrequency=250ms",
      "pipelite.task.retries=1"
    })
@ContextConfiguration(initializers = OracleSuccessPipeliteLauncherTest.TestContextInitializer.class)
@ActiveProfiles(value = {"oracle-test"})
public class OraclePollablePipeliteLauncherTest {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private TaskConfiguration taskConfiguration;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;
  @Autowired private ProcessService processService;
  @Autowired private TaskService taskService;
  @Autowired private LockService lockService;

  public OraclePollablePipeliteLauncherTest() {}

  public static class TestContextInitializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues.of(
              "pipelite.launcher.launcherName=" + UniqueStringGenerator.randomLauncherName(),
              "pipelite.process.processName=" + UniqueStringGenerator.randomProcessName())
          .applyTo(configurableApplicationContext.getEnvironment());
    }
  }

  @Test
  public void testPollSuccessExecuteSuccess() {
    PollablePipeliteLauncherTester tester =
        new PollablePipeliteLauncherTester(
            launcherConfiguration,
            processConfiguration,
            taskConfiguration,
            processService,
            taskService,
            lockService);
    tester.testPollSuccessExecuteSuccess();
  }

  @Test
  public void testPollErrorExecuteSuccess() {
    PollablePipeliteLauncherTester tester =
        new PollablePipeliteLauncherTester(
            launcherConfiguration,
            processConfiguration,
            taskConfiguration,
            processService,
            taskService,
            lockService);
    tester.testPollErrorExecuteSuccess();
  }

  @Test
  public void testPollErrorExecuteError() {
    PollablePipeliteLauncherTester tester =
        new PollablePipeliteLauncherTester(
            launcherConfiguration,
            processConfiguration,
            taskConfiguration,
            processService,
            taskService,
            lockService);
    tester.testPollErrorExecuteSuccess();
  }

  @Test
  public void testPollExceptionExecuteSuccess() {
    PollablePipeliteLauncherTester tester =
        new PollablePipeliteLauncherTester(
            launcherConfiguration,
            processConfiguration,
            taskConfiguration,
            processService,
            taskService,
            lockService);

    tester.testPollExceptionExecuteSuccess();
  }

  @Test
  public void testPollExceptionExecuteError() {
    PollablePipeliteLauncherTester tester =
        new PollablePipeliteLauncherTester(
            launcherConfiguration,
            processConfiguration,
            taskConfiguration,
            processService,
            taskService,
            lockService);

    tester.testPollExceptionExecuteError();
  }
}
