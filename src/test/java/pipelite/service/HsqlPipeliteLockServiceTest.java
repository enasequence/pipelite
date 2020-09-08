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
import pipelite.FullTestConfiguration;

@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test"})
public class HsqlPipeliteLockServiceTest {

  @Autowired PipeliteLockService service;

  @Test
  @Transactional
  @Rollback
  public void test() {
    PipeliteLockServiceTester.testLaucherLocks(service);
  }

  @Test
  @Transactional
  @Rollback
  public void testProcessLocks() {
    PipeliteLockServiceTester.testProcessLocks(service);
  }
}
