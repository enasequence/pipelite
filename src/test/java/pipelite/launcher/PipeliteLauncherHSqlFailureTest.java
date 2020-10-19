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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import pipelite.TestConfiguration;
import pipelite.configuration.ProcessConfiguration;

import javax.annotation.PostConstruct;

@SpringBootTest(
    classes = TestConfiguration.class,
    properties = {
      "pipelite.launcher.processLaunchParallelism=5",
      "pipelite.launcher.processLaunchFrequency=250ms",
      "pipelite.launcher.stageLaunchFrequency=250ms",
      "pipelite.stage.maximumRetries=1"
    })
@ContextConfiguration(initializers = PipeliteLauncherHSqlSuccessTest.TestContextInitializer.class)
@ActiveProfiles(value = {"hsql-test"})
public class PipeliteLauncherHSqlFailureTest {

  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  private PipeliteLauncherFailureTester tester;

  @PostConstruct
  public void init() {
    tester =
        new PipeliteLauncherFailureTester(processConfiguration, pipeliteLauncherObjectProvider);
  }

  @Test
  public void testFirstStageFails() {
    tester.testFirstStageFails();
  }

  @Test
  public void testSecondStageFails() {
    tester.testSecondStageFails();
  }

  @Test
  public void testThirdStageFails() {
    tester.testThirdStageFails();
  }

  @Test
  public void testFourthStageFails() {
    tester.testFourthStageFails();
  }

  @Test
  public void testNoStageFails() {
    tester.testNoStageFails();
  }
}
