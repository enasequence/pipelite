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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.launcher.pipelineParallelism=2",
      "pipelite.launcher.processLaunchFrequency=250ms",
      "pipelite.launcher.stageLaunchFrequency=250ms",
      "pipelite.launcher.shutdownIfIdle=true"
    })
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
@DirtiesContext
public class PipeliteLauncherHSqlAsyncTest {

  @Autowired private PipeliteLauncherAsyncTester tester;

  @Test
  public void testSubmitSuccessPollSuccess() {
    tester.testSubmitSuccessPollSuccess();
  }

  @Test
  public void testSubmitError() {
    tester.testSubmitError();
  }

  @Test
  public void testSubmitException() {
    tester.testSubmitException();
  }

  @Test
  public void testPollError() {
    tester.testPollError();
  }

  @Test
  public void testPollException() {
    tester.testPollException();
  }
}
