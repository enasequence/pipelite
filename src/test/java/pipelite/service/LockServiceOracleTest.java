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
package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfiguration;
import pipelite.configuration.LauncherConfiguration;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"oracle-test", "pipelite-test"})
public class LockServiceOracleTest {

  @Autowired LockService service;
  @Autowired LauncherConfiguration launcherConfiguration;

  @Test
  @Transactional
  @Rollback
  public void testLauncherLocks() {
    LockServiceTester.testLauncherLocks(service, launcherConfiguration.getPipelineLockDuration());
  }

  @Test
  @Transactional
  @Rollback
  public void testProcessLocks() {
    LockServiceTester.testProcessLocks(service);
  }
}
